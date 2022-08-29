from __future__ import annotations


# TODO get rid of this global variable
line_num = 0

dbpassword = None

def getDbPassword():
    """
    Get the database password from the user and save for next time
    """
    global dbpassword
    if dbpassword:
        return dbpassword
    else:
        dbpassword = input("Please enter the database password ")
        return dbpassword

# to be filled in corresponding fileN.py files
zip_filenames_dict = {}
passwords_dict = {}


def get_zip_password(infpath):
    """
    Get zip password from passwords_dict, 
    taking into account only the file name and not the directory part of its path
    """
    import os
    return passwords_dict[os.path.basename(infpath)]


def get_filename_in_zip(infpath):
    """
    Get file path of CSV file within the zip file from zip_filenames_dict, 
    taking into account only the file name and not the directory part of its path
    """
    import os
    return zip_filenames_dict[os.path.basename(infpath)]

def split_row_generator(infpaths, colsep, print_headers = True):
    """
    Generates rows as lists of cells in the CSVs in the zip files.
    Usage: for row in split_row_generator(['path/to/zip',], ',', True): do_stuff(row).
    Do not do list(split_row_generator(...)), the rows won't fit into memory 
    """
    # for each zip file
    for infpath in infpaths:
        import zipfile
        with zipfile.ZipFile(infpath) as zf: 
            # open the csv file in the zip file
            with zf.open(get_filename_in_zip(infpath), pwd = get_zip_password(infpath)) as infile:
                # skip the first line of headers
                headers = next(infile)
                if print_headers:
                    print(headers)

                # for each line
                for line in infile:
                    # inrement line counter
                    global line_num
                    line_num += 1

                    # decode line (stored as bytes in zip, want string)
                    line = line.decode('utf-8')
                    linestrip = line.rstrip() # remove trailing newline
                    split = linestrip.split(colsep) # split on the column separator
                    yield split


import concurrent.futures
class CsvDatabaseWriterThreaded:
    """
    A class to manage writing into a temporary csv and then the database.
    Can be used similarly to writing to just a csv file. (Using python's 'with' block)
    """
    def __init__(self, outfpath, batch_size, dbname, table_name, timeout_s = 10) -> None:
        self.executor = None
        self.latest_future = None
        self.swapfpath = outfpath + '_2' # creates a second temporary file (one for each thread)
        self.timeout_s = timeout_s

        self.dbname = dbname
        self.batch_size = batch_size
        self.outfpath = outfpath
        self.table_name = table_name
        self.line_counter = 0

    def __enter__(self):
        """Enables python 'with' block functionality"""
        self.executor = concurrent.futures.ThreadPoolExecutor()
        self.outfile = open(self.outfpath, 'w')
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Enables python 'with' block functionality"""

        # check if any lines left in current batch before exiting
        if self.line_counter % self.batch_size != 0:
            # save if there are
            self._save_to_db_single(self.outfile, self.outfpath)
        # tell file and thread executor to also stop
        self.outfile.__exit__(exc_type, exc_value, traceback)
        self.executor.__exit__(exc_type, exc_value, traceback)
        # TODO delete temporary files

    def open(self, ):
        self.__enter__()
        return self.outfile

    def close(self):
        return self.__exit__(None, None, None)

    def _save_to_db_single(self, file_context, file_name, num_header_lines = 0):
        """
        Save temporary csv file into database. 
        Parameters need to be passed in instead of using class fields 
        because those will be changed in another thread while this is running. (Race conditions) 
        """
        # close the temporary csv file, makes sure everything is written in it.
        file_context.close()

        # run the command in bash to import csv into database (wait until finished)
        import subprocess
        subprocess.Popen((f'clickhouse-client --password={getDbPassword()}'
        ' --format_csv_delimiter="|"'
        f' --input_format_csv_skip_first_lines={num_header_lines}'
        f' --query="INSERT INTO {self.dbname}.{self.table_name} FORMAT CSV" < "{file_name}"'),
        shell=True).wait()
        # on second thought, it would likely be faster to call executemany() on the clickhouse python client



    def re_open(self):
        """Open temp csv file for writing"""
        self.outfile = open(self.outfpath, 'w')

    def _save_to_db_threaded(self, *args, **kwargs):
        """Save the current temp csv file into database in a separate thread"""
        # wait until last thread finished
        if self.latest_future is not None:
            self.latest_future.result(self.timeout_s)

        # save the current temporary csv into db
        self.latest_future = self.executor.submit(self._save_to_db_single,
            self.outfile, self.outfpath, *args, **kwargs)
        # swap temporary csv file paths
        self.outfpath, self.swapfpath = self.swapfpath, self.outfpath
        # re_open happens in write()

    def write(self, line, colsep):
        """
        Write a line to the database (via a temporary csv file).
        The colsep parameter should be a single character that does not appear anywhere in line.
        """
        return_value = self.outfile.write(colsep.join(line)+ '\n')
        # save csvs to database if batch filled
        self.line_counter += 1
        if self.line_counter % self.batch_size == 0:
            self._save_to_db_threaded()
            self.re_open()
        return return_value


class ErrorHandler:
    """
    A class to help handle errors, writing them to a file or crashing if a limit is reached
    """
    def __init__(self, errorfpath, max_errors_before_crash = None, colsep = '|') -> None:
        self.errorfpath = errorfpath
        self.max_errors_before_crash = max_errors_before_crash
        self.colsep = colsep
        self.error_count = 0
        pass
    
    # for 'with' block
    def __enter__(self):
        self.open()
        return self
    def __exit__(self, type = None, value = None, traceback = None):
        self.errorfile.__exit__(type, value, traceback)
        

    def open(self):
        self.errorfile = open(self.errorfpath, 'w')
        return self.errorfile

    def close(self, *args, **kwargs):
        return self.__exit__()

    def line_arr_to_str(self, line_arr):
        return self.colsep.join(line_arr)

    def save_and_ignore_until_max(self, line):
        # convert line to string if it is a list
        if isinstance(line, list):
            line = self.line_arr_to_str(line)
        print(line)
        # getting the exception stack trace is weird in python, needs this import
        import traceback
        tb = traceback.format_exc()
        print(tb)
        self.errorfile.write(line)
        self.errorfile.write('#')
        self.errorfile.write(str(tb))
        self.errorfile.write('\n')
        self.error_count += 1
        if self.max_errors_before_crash is not None:
            if self.error_count > self.max_errors_before_crash:
                raise 



class DBInfo:
    """
    A way to keep database info in one place.
    Be VERY careful with this class, it may lead to SQL injection attacks 
    if any user-provided data is used here. 
    (Limiting types to string literals is not available until Python 3.11)
    """
    def __init__(self, creation_string_columns, dbname = None, table_name = None) -> None:
        self.dbname = dbname
        self.table_name = table_name
        self.creation_string_columns = creation_string_columns

    def _ensure_fields_not_None(self):
        if self.creation_string_columns is None:
            raise RuntimeError('DBInfo creation_string_columns is not set.')
        if self.dbname is None:
            raise RuntimeError('DBInfo dbname is not set.')
        if self.table_name is None:
            raise RuntimeError('DBInfo table_name is not set.')


    def csv_to_db_create(self, drop_if_exists = True):
        
        self._ensure_fields_not_None()
        import os

        if drop_if_exists:
            os.system(f'clickhouse-client --password={getDbPassword()}'
            f' --query="DROP TABLE IF EXISTS {self.dbname}.{self.table_name};"')

        os.system(f'clickhouse-client --password={getDbPassword()}'
            ' --query="'
            f"CREATE TABLE {self.dbname}.{self.table_name} ("
            f'{self.creation_string_columns}'
            ';"')

def apply_and_save(infpaths, row_function, dbinfo, *extra_params, **extra_named_parameters):
    """
    Apply row_function to every row in CSVs in zips in infpaths, and save results to db like dbinfo.
    Any extra parameters will be passed to row_function
    """
    # set some parameters
    outfpath = 'data/tmp_to_save.csv'
    errorfpath = 'error_file.txt'
    max_errors_before_crash = 100
    if dbinfo.dbname is None:
        dbinfo.dbname = 'nr'    
    dbinfo.csv_to_db_create() # consider moving this to CsvDatabaseWriter, with an optional append vs replace parameter
    colsep = '|'
    csv_batch_size = 100000
    timeout_s=60

    # open database writer and error handler
    with CsvDatabaseWriterThreaded(outfpath, csv_batch_size, dbinfo.dbname, dbinfo.table_name, timeout_s=timeout_s) as writer, \
            ErrorHandler(errorfpath, max_errors_before_crash, colsep) as error_handler:
        # for each row in input file
        for split in split_row_generator(infpaths, colsep):
            try:

                # apply row function
                conv = row_function(split, *extra_params, **extra_named_parameters)
                
                # save result
                writer.write(conv, colsep)

            except KeyboardInterrupt as ki:
                # propagate KeyboardInterrupt (Ctrl+C cancel) out to stop execution
                print(f'KeyboardInterrupt at infile line {line_num}')
                raise
            except Exception as e:
                # don't crash on results, save to a separate file instead
                error_handler.save_and_ignore_until_max(split)
                pass
        print(f'Loaded {writer.line_counter} lines total into the database.')

