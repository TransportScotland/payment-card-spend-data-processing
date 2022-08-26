from __future__ import annotations


# TODO get rid of this global variable
line_num = 0

dbpassword = 'RadicalSpiderWearingPaper'

def getDbPassword():
    global dbpassword
    if dbpassword:
        return dbpassword
    else:
        dbpassword = input("Please enter the database password ")
        return dbpassword

# to be filled in corresponding fileN.py files
passwords_dict = {}
zip_filenames_dict = {}

#TODO load from a file
def get_zip_pwd(infpath):
    import os
    return passwords_dict[os.path.basename(infpath)]


def get_filename_in_zip(infpath):
    import os
    return zip_filenames_dict[os.path.basename(infpath)]

def split_row_generator(infpaths, colsep, print_headers = True):
    for infpath in infpaths:
        import zipfile
        with zipfile.ZipFile(infpath) as zf:
            # with zf.open(zf.namelist()[0], pwd = get_zip_pwd(infpath)) as infile: 
            with zf.open(get_filename_in_zip(infpath), pwd = get_zip_pwd(infpath)) as infile:
                headers = next(infile)
                if print_headers:
                    print(headers)

                for line in infile:
                    global line_num
                    line_num += 1

                    # if isinstance(line, bytes):
                    line = line.decode('utf-8')
                    linestrip = line.rstrip()
                    split = linestrip.split(colsep)
                    yield split


import concurrent.futures
class CsvDatabaseWriterThreaded:
    def __init__(self, outfpath, batch_size, dbname, table_name, timeout_s = 10) -> None:
        self.executor = None
        self.latest_future = None
        self.swapfpath = outfpath + '_2'
        self.timeout_s = timeout_s
        # super().__init__(outfpath, batch_size, dbname, table_name)

        self.dbname = dbname
        self.batch_size = batch_size
        self.outfpath = outfpath
        self.table_name = table_name
        self.line_counter = 0

    def __enter__(self):
        self.executor = concurrent.futures.ThreadPoolExecutor()
        # return super().__enter__()
        self.open()
        return self

    def __exit__(self, exc_type, exc_value, traceback):

        # first check if any lines left in current batch
        if self.line_counter % self.batch_size != 0:
            # self.close_file_for_saving()
            self._save_to_db_single(self.outfile, self.outfpath)
        self.outfile.__exit__(exc_type, exc_value, traceback)
        self.executor.__exit__(exc_type, exc_value, traceback)
        # return super().__exit__(exc_type, exc_value, traceback)

    def open(self, ):
        self.outfile = open(self.outfpath, 'w')
        return self.outfile

    def close(self):
        return self.__exit__(None, None, None)

    def _save_to_db_single(self, file_context, file_name, num_header_lines = 0):
        # need to take in the file_context and file_name as parameters to avoid race conditions
        file_context.close()

        import subprocess
        subprocess.Popen((f'clickhouse-client --password={getDbPassword()}'
        ' --format_csv_delimiter="|"'
        f' --input_format_csv_skip_first_lines={num_header_lines}'
        f' --query="INSERT INTO {self.dbname}.{self.table_name} FORMAT CSV" < "{file_name}"'),
        shell=True).wait()



    def re_open(self):
        self.outfile = open(self.outfpath, 'w')

    def _save_to_db_threaded(self, *args, **kwargs):
        # wait until last thread finished
        if self.latest_future is not None:
            self.latest_future.result(self.timeout_s)

        self.latest_future = self.executor.submit(self._save_to_db_single,
            self.outfile, self.outfpath, *args, **kwargs)
        self.outfpath, self.swapfpath = self.swapfpath, self.outfpath
        # re_open happens in write()

    def write(self, line, colsep):
        return_value = self.outfile.write(colsep.join(line)+ '\n')
        # save to csv if batch filled
        self.line_counter += 1
        if self.line_counter % self.batch_size == 0:
            # self.close_file_for_saving()
            self._save_to_db_threaded()
            self.re_open()
        return return_value


class ErrorHandler:
    def __init__(self, errorfpath, max_errors_before_crash = None, colsep = '|') -> None:
        self.errorfpath = errorfpath
        self.max_errors_before_crash = max_errors_before_crash
        self.colsep = colsep
        self.error_count = 0
        pass
    
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
        pass
        if isinstance(line, list):
            line = self.line_arr_to_str(line)
        print(line)
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
    Be VERY careful with this class, it may lead to SQL injection attacks 
    if any user-provided data is used here. 
    (Limiting types to string literals will become available in Python 3.11)
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

    # todo look into alternative file types for the intermediate file, csv too big (zipped may be fine)
    # outfpath = '/mnt/sftp/module 1/tmp_module1_t1.csv'
    outfpath = 'data/tmp_to_save.csv'
    errorfpath = 'error_file.txt'
    max_errors_before_crash = 100
    if dbinfo.dbname is None:
        dbinfo.dbname = 'nr'
    # consider moving this to CsvDatabaseWriter, with an optional append vs replace parameter
    dbinfo.csv_to_db_create()
    colsep = '|'


    with CsvDatabaseWriterThreaded(outfpath, 100000, dbinfo.dbname, dbinfo.table_name, timeout_s=60) as writer, \
            ErrorHandler(errorfpath, max_errors_before_crash, colsep) as error_handler:
        for split in split_row_generator(infpaths, colsep):
            try:

                conv = row_function(split, *extra_params, **extra_named_parameters)
                
                writer.write(conv, colsep)

            except KeyboardInterrupt as ki:
                print(f'KeyboardInterrupt at infile line {line_num}')
                raise ki
            except Exception as e:
                error_handler.save_and_ignore_until_max(split)
                pass
        print(f'Loaded {writer.line_counter} lines into the database.')

