# Functions common to all four of the fact table processing scripts.
# Most of the functionality is in here or at least goes through here.
# This could likely be reused as a library for other ETL tasks by updating table_info.py and dimension.py to match different ETL tasks


# import python libraries
import time
import MySQLdb
from collections import deque

# import my python modules
import dimension
import table_info


# dimension data structures like {name: dimension_object}
dicts = {} #TODO rename this

# todo probably better saving these to a file directly. 
# also adding a max error limit to avoid going through the whole dataset doing nothing
# for error handling, unused.
error_rows = []
# for expectedly skipped rows. currently also unused.
skipped_rows = []
district_rows = []

# TODO consider renaming places where dim_dict is to just dimension and similar (this file's  object dicts)

# mostly for backwards compatibility purposes atm
def _new_dim_dict(name):
    """
    Create a dimension object and add it to this module's dimension dictionary under the name provided.
    If name is 'time', creates TimeDimension, if 'location' LocationDimension, otherwise SimpleDimension.

    Raises ValueError is a dimension with this name already exists in the dictionary.
    """

    # todo consider checking against table_info.ALLOWED_TABLES before creating

    if name in dicts:
        raise ValueError(f'Dict with this name already exists: {name}')
    if name == 'time':
        dicts[name] = dimension.TimeDimension()
    elif name == 'location':
        dicts[name] = dimension.LocationDimension()
    else:
        dicts[name] = dimension.SimpleDimension()


# consider combining with _new_dim_dict into one
def _ensure_dim_dict(name):
    """Calls _new_dim_dict if a dimension with this name does not exist already."""
    if name not in dicts:
        _new_dim_dict(name)

# consider stopping supporting this functionality, it's not even that important
def _unwrap_wrap_args(*args):
    """Turns arguments like ('a', ['b', 'c'], 'd') into list ['a', 'b', 'c', 'd']."""
    out = []
    for arg in args:
        if type(arg) == list or type(arg) == tuple:
            for element in arg:
                out.append(element)
        else:
            out.append(arg)
    return out

def ensure_dim_dicts(*dimension_names):
    """
    Creates dimension objects if ones with this name do not exist yet.
    Expected names of dimensions are in table_info.ALLOWED_TABLES.
    """
    
    # in retrospect, accepting either a list or a *args instead of just one of them is a weird way to do things.
    dims_list = _unwrap_wrap_args(dimension_names)
    for dim in dims_list:
        _ensure_dim_dict(dim)


def split_line(line):
    """Remove trailing whitespace and split on every comma"""
    return line.rstrip().split(',')


def create_db_table(table_name, dbcon, drop_if_exists = True):
    """Create a table in the database using table_info.create_string[table_name]."""
    c = dbcon.cursor()
    if (drop_if_exists):
        # maybe be more carfeul with dropping in production
        # also maybe escape
        c.execute(f'DROP TABLE IF EXISTS {table_name}')
        # c.execute('DROP TABLE IF EXISTS %s', table_name)
    c.execute(table_info.create_strings[table_name])


def __raise_not_numeric(row_cell, row):
    raise ValueError('All elements in the fact table must be numerical to prevent SQL injection attacks.' +
        f'Instead found: element {row_cell} of type {type(row_cell)} in row {row}')


def _raise_if_not_int_float(list_of_rows):
    for row in list_of_rows:
        for row_cell in row:
            if(not(type(row_cell) == int or type(row_cell) == float)):
                # raise an exception if not a number but chceck numeric strings
                if (type(row_cell) == str):
                    try:
                        # throw a ValueError if string not convertible to float
                        _ = float(row_cell)
                    except ValueError:
                        __raise_not_numeric(row_cell, row)
                else:
                    __raise_not_numeric(row_cell, row)

def save_batch(batch, dbcon, table_name):
    """
    Save a batch of fact table rows into the database.

    CAREFUL: MAY be susceptible to SQL injection attacks. 
        Does not use existing library functions to escape any characters but only accepts int or float.
        So should be safe but do procees with caution.
    """

    values = batch

    # prepare the headers part of the SQL query
    headers = table_info.headers_dict[table_name]
    headers_str = '('+ ','.join(headers) + ')'

    sql_base = f"INSERT INTO {table_name} {headers_str} VALUES "

    # raise an Exception if anything of type other than (float, int) comes in
    # be very careful removing the following line, will need to escape string if anything other than numbers comes in.
    _raise_if_not_int_float(values)

    # each row in values is a tuple so str(v) is (e1, e2, e3, ...) already, and just put commas in between each
    values_str = ','.join([str(v) for v in values])
    bigsql = sql_base + values_str
    if values_str == "":
        raise Exception("batch was empty in save_batch")
    
    dbcon.cursor().execute(bigsql)
    dbcon.commit()

def save_batch_timed(*args):
    """Call save_batch and time how long it took"""
    start_time = time.time()
    save_batch(*args)
    end_time = time.time()
    return end_time - start_time


#TODO comments and better names

def _read_filestream_as_split_lines(file):
    for line in file:
        yield split_line(line)

def read_file_as_split_lines(filepath, skip_headers = True):
    with open(filepath) as file:
        if skip_headers:
            _ = next(file)
        
        yield from _read_filestream_as_split_lines(file)

def batch_process_threaded_from_file(path, skip_headers=True, **kwargs):
    batch_process_threaded_from_generator(read_file_as_split_lines(path, skip_headers), **kwargs)

def does_table_exist(table_name, dbcon):
    c = dbcon.cursor()
    c.execute("show tables like %s", (table_name,))
    res = c.fetchone()
    res = res[0]
    return True if res == table_name else False

def ensure_table(table_name, dbcon, if_exists):
    if if_exists == 'replace':
        return create_db_table(table_name, dbcon)
    elif if_exists == 'append':
        if not does_table_exist(table_name, dbcon):
            # only create if it doesn't exist
            create_db_table(table_name, dbcon)
    elif if_exists == 'fail':
        if does_table_exist(table_name, dbcon):
            raise ValueError(f'Table {table_name} already exists')

import concurrent.futures
def batch_process_threaded_from_generator(row_generator, row_func, fact_table_name, dbcon, if_exists='fail'):
    start_time = time.time()
    maxlen = 4000 # 16s. 1000 21s, 10000 17s
    batch = deque([], maxlen=maxlen)
    # also consider multiprocessing Queue - one shared queue between threads, and just save as it fills up (or like a queue of batches even)

    # connect if dbcon is null
    connected_here = False
    if not dbcon:
        dbcon = connect_to_db()
        connected_here = True

    ensure_table(fact_table_name, dbcon, if_exists)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        latest_future = None
        total_insert_time = 0

        for row in row_generator:
            # also option of using csv.reader - slower but handles quotation marks around cells

            # maybe make fact_line come from a generator instead, to separate creating and saving
            try:
                fact_line = row_func(row)
                # except Exception as e:
                #     error_rows.append((row, e))

                batch.append(fact_line)
            except dimension.DistrictException as de:
                district_rows.append(row)
                continue # skip the rest of the execution for this line here
            except Exception as e:
                error_rows.append((row, e))
            if len(batch) == maxlen:

                if latest_future:
                    # wait up to 2 seconds for the last SQL INSERT to finish (or raise an Exception)
                    time_taken = latest_future.result(2)
                    total_insert_time += time_taken

                latest_future = executor.submit(save_batch_timed, batch, dbcon, fact_table_name)
                batch = deque([], maxlen=maxlen) # empty the queue here

        # wait for last save task to finish
        if latest_future:
            time_taken = latest_future.result(2)
            total_insert_time += time_taken

        # save items left in the queue after finished reading the file (e.g. if there are 9500 rows and batches are size 1000, 500 left at the end)
        if not len(batch) == 0:
            latest_future = executor.submit(save_batch_timed, batch, dbcon, fact_table_name)
            time_taken = latest_future.result(2)
            total_insert_time += time_taken

        # close connection if connected here
        if (connected_here):
            dbcon.close()
    end_time = time.time()
    print(f'read handle and write took {end_time - start_time} seconds to table {fact_table_name}.')
    print(f'Inserting into fact table took a total of {total_insert_time} seconds')
    pass  


# TODO rename this
# TODO refactor needed, there is a lot of deeply nested function calls where each don't really add anything 
def batch_process(path, row_func, fact_table_name, dbcon=None):
    """Process file at path. See batch_process_threaded"""
    batch_process_threaded_from_file(path=path, skip_headers=True, row_func=row_func, fact_table_name=fact_table_name, dbcon=dbcon, if_exists='replace')
    pass


def save_dim(name, dbcon = None):
    """Wrapper for Dimension.to_sql, also creates connection if dbcon null"""
    db_connected_here = False
    if not dbcon:
        dbcon = connect_to_db()
        db_connected_here = True

    dicts[name].to_sql(name, dbcon)
    if db_connected_here:
        dbcon.close()

def save_dims():
    """Save all the created dimensions into SQL database"""
    dbcon = connect_to_db()
    for name in dicts.keys():
        save_dim(name, dbcon)
    dbcon.close()

# TODO make this adjustable
def connect_to_db() -> MySQLdb.Connection:
    """Get MySQLdb connection to default database."""
    db = MySQLdb.connect(user='temp_user', passwd = 'password', database='sgov')
    # c = db.cursor()
    return db

def get_sqlalchemy_con():
    """Get SQLAlchemy engine for the database"""
    import sqlalchemy
    return sqlalchemy.create_engine('mysql://', creator=connect_to_db)


# A lot of wrapper functions which are mostly here for historic purposes. But may be good to keep anyway in case changes are wanted.

def add_to_dict_if_not_in(dict_name, key, values: list):
    """Wrapper function for dimension.add_if_not_in. Takes in name of dim as stored in dicts here"""
    return dicts[dict_name].add_if_not_in(key, values)

 
def handle_one_dim(dict_name, row, row_index):
    """Wrapper for SimpleDimension.add_row. Takes in name of dim as stored in dicts here"""
    return dicts[dict_name].add_row(row, row_index)

def handle_time(row, index = 0):
    """Wrapper for TimeDimension.add_row. Takes in name of dim as stored in dicts here"""
    return dicts['time'].add_row(row, index)

def handle_loc(row, loc_level_idx = 4, loc_idx = 5, smallest_area_name = 'POSTCODE_SECTOR'):
    """Wrapper for LocationDimension.add_row. Takes in name of dim as stored in dicts here"""
    return dicts['location'].add_row(row, loc_level_idx, loc_idx, smallest_area_name)

# consider removing
def postcode_sector_to_loc_list(sector: str):
    """Wrapper for LocationDimension.postcode_sector_to_loc_list."""
    return dimension.LocationDimension.postcode_sector_to_loc_list(sector)

def get_headers(dict_name):
    """Get the headers of a given dimension"""
    return dicts[dict_name].headers


