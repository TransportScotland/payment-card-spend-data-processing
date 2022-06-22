import pandas as pd
import os
import time
import csv
import MySQLdb
# import queue # adds a TON of overhead, only for multiprocessing
from collections import deque

import table_info


dicts = {}
dict_maxids = {}
dict_headers = {}

skipped_rows = []

def add_to_dict_if_not_in(dict_name, key, values: list):
    if key not in dicts[dict_name]:
        values.append(dict_maxids[dict_name])
        dicts[dict_name][key] = values
        dict_maxids[dict_name] = dict_maxids[dict_name] + 1
        return values[-1]
    else:
        return dicts[dict_name][key][-1] # return id of new key

def handle_one_dim(dict_name, row, row_index):
    val = row[row_index]
    id = add_to_dict_if_not_in(dict_name, val, [val])
    return id


def postcode_sector_to_loc_list(sector: str):
    # district = sector.rsplit(' ', maxsplit=1)[0]
    district = sector[:-2]

    # first digit if second digit is number, else first two digits.
    # to confirm if any postcodes areas are 3 chars (simple loop then)
    area = district[0] if district[1].isnumeric() else district[0:2]

    # region might have to be done differently (current only known one is scotland)
    region = 'unknown'

    return [sector, district, area, region]

# removing increases speed by 15%
def handle_time(row, index = 0):
    time_raw = str(row[index]) #optional depending on stuff
    if (not time_raw[0].isnumeric()):
        # a quarter - skipping for now
        raise
    year = int(time_raw[:4]) #check if faster this or 202201 / 100 and 202201 % 100
    month = int(time_raw[4:6])
    quarter = int(month / 3)

    # not sure if I actually need time_raw in the table but maybe there was a reason for it
    time_id = add_to_dict_if_not_in('time', time_raw, [time_raw, year, quarter, month])
    return time_id
    
# 15%
def handle_loc(row, loc_level_idx = 4, loc_idx = 5, smallest_area_name = 'POSTCODE_SECTOR'):
    # there may be a better way to do this than a star schema
    if row[loc_level_idx] != smallest_area_name:
        # probably skip adding this line to the fact table.
        # but do save it somewhere to check the sub-sections add up to the right number
        # skip handling this until data is known
        return -1
    sector = row[loc_idx]
    loc_list = postcode_sector_to_loc_list(sector)

    id = add_to_dict_if_not_in('location', sector, loc_list)
    return id


def new_dim_dict(name):
    # this lowkey begs for being a class
    # definitely dict_info and maybe this whole file
    if name in dicts:
        raise ValueError(f'Dict with this name already exists: {name}')
    dicts[name] = {}
    dict_maxids[name] = 0
    dict_headers[name] = table_info.headers_dict[name]

def _ensure_dim_dict(name):
    if name not in dicts:
        new_dim_dict(name)


def ensure_dim_dicts(*dims_list):
    # if type(dims_list) == list:
    for dim in dims_list:
        if type(dim) == list or type(dim) == tuple:
            # unwrap list or tuple arguments (not all iterables)
            for d in dim:
                _ensure_dim_dict(d)
        else:
            _ensure_dim_dict(dim)


def split_line(line):
    return line.rstrip().split(',')


def create_db_table(table_name, dbcon, drop_if_exists = True):
    c = dbcon.cursor()
    if (drop_if_exists):
        # maybe be more carfeul with dropping in production
        # also maybe escape
        c.execute(f'DROP TABLE IF EXISTS {table_name}')
        # c.execute('DROP TABLE IF EXISTS %s', table_name)
    c.execute(table_info.create_strings[table_name])




def save_batch(batch, dbcon, table_name):
    values = [batch.popleft() for _ in range(len(batch))]

    headers = table_info.headers_dict[table_name]
    headers_str = '('+ ','.join(headers) + ')'
    values_f_str = '(' + ','.join(['%s' for _ in range(len(headers))]) + ')'
    values_f_str = '' # constructing a full command instead of parameters

    sql = f"INSERT INTO {table_name} {headers_str} VALUES {values_f_str}"
    # dbcon.cursor().executemany(sql, values)

    # may need to escape if anything other than numbers comes in.
    # this is where it would've been useful to have types
    # values_str = ','.join(['('+','.join([dbcon.escape_string(str(e)).decode('ascii') for e in v]) + ')' for v in values])
    assert(all(type(e) for e in (int, float) for row in values for e in row))

    values_str = ','.join([str(v) for v in values])
    bigsql = sql + values_str
    dbcon.cursor().execute(bigsql)


def batch_process(path, row_func, fact_table_name, dbcon = None):
    time0 = time.time()
    with open(path) as file:
        headers = next(file) # discard first row
        # batch = queue.Queue(2000)
        batch = deque([], maxlen=1000) # probably better separating the maxlen from the queue when using collections.deque
        if not dbcon:
            dbcon = connect_to_db()
        
        create_db_table(fact_table_name, dbcon)
        for line in file:
            row = split_line(line)

            fact_line = row_func(row)

            batch.append(fact_line)
            if len(batch) == batch.maxlen:
                save_batch(batch, dbcon, fact_table_name)
                # dbcon.commit()

        # save remaining items in the queue
        if not len(batch) == 0:
            save_batch(batch, dbcon, fact_table_name)
        dbcon.commit()
        dbcon.close()
    time1 = time.time()
    print(f'read handle and write took {time1-time0} seconds on file {os.path.basename(path)}.')


def save_dim(name, dbcon):
    # values = [batch.popleft() for _ in range(len(batch))]
    values = [tuple(row[:-1])for row in dicts[name].values()]
    headers = table_info.headers_dict[name][:-1]

    # headers = headers_dict[table_name]
    headers_str = '('+ ','.join(headers) + ')'
    values_f_str = '(' + ','.join(['%s' for _ in range(len(headers))]) + ')'

    sql = f"INSERT INTO {name} {headers_str} VALUES {values_f_str}"
    dbcon.cursor().executemany(sql, values)

def save_dims():
    dbcon = connect_to_db()
    for name in dicts.keys():
        create_db_table(name, dbcon)
        save_dim(name, dbcon)
    dbcon.commit()
    dbcon.close()

def connect_to_db():
    db = MySQLdb.connect(user='temp_user', passwd = 'password', database='sgov')
    # c = db.cursor()
    return db


def get_headers(dict_name):
    return dict_headers[dict_name]
















# (python has no built-in deprecated tag without installing a package so keeping these here for now)
# depracated:

def dict_to_df(dict_name):
    return pd.DataFrame(data= dicts[dict_name].values(), columns = dict_headers[dict_name])

def list_to_df(list, headers):
    return pd.DataFrame(data=list, columns=headers)

def dims_to_csv(folder):
    if not os.path.exists(folder):
        os.makedirs(folder)

    for name in dicts.keys():
        df = dict_to_df(name)
        df.to_csv(os.path.join(folder, f'{name}.csv'))

def list_to_csv(list, headers, folder, name):
    if not os.path.exists(folder):
        os.makedirs(folder)
    df = list_to_df(list, headers)
    df.to_csv(os.path.join(folder, name), index = False)



# maybe use a generator instead
def read_and_handle(path, row_func, fact_list, skip_n_rows = 1, skip_after_0_empty = False):
    time0 = time.time()
    skipped_rows_added = False
    with open(path) as file:
        for i in range(skip_n_rows):
            headers = file.__next__().rstrip().split(',')#[1:] #skipping col 0 for now
            # print(headers)

        # using csv reader slows down execution by like 30% - from 5.2s to 7.2s
        # but it is somewhat necessary for the census data
        
        csvreader = csv.reader(file)
        for row in csvreader:
        # for line in file:
            # stripped = line.strip()
            # row = stripped.split(',')#[1:]
            # print(stripped) 
            if skip_after_0_empty and not row[0]:
                break
            try:
                row_func(row, fact_list)
            except Exception as e:
                # todo make my own exceptions to not mix them up with built-in ones
                print(e)
                if not skipped_rows_added:
                    skipped_rows.append(f'#### Skipped rows of {path}:')
                    skipped_rows_added = True
                skipped_rows.append(row)

    time1 = time.time()
    print(f'read and handle took {time1-time0} seconds on file {os.path.basename(path)}.')
        # if skipped_rows:
        #     print()
        #     print('skipped rows:')
        #     for r in skipped_rows:
        #         print(r)


def save_batch_csv(batch, path):
    def row_to_saveable(row):
        return ','.join([str(e) for e in row]) + '\n'
    # save_lines = [','.join(str(batch.get_nowait())) for _ in range(batch.qsize())]
    # save_lines = [','.join(row) for row in batch]
    # save_str = '\n'.join(save_lines)
    # save_lines = [row_to_saveable(batch.popleft()) for _ in range(len(batch))]
    save_lines = [row_to_saveable(batch.popleft()) for _ in range(len(batch))]
    # save_lines = [row_to_saveable(batch.get_nowait()) for _ in range(batch.qsize())]
    # print(save_lines)
    with open(path, 'a') as file:
        #probably instead open the file/connection at the beginning and close at the end
        # file.write(save_str)
        file.writelines(save_lines)

def save_csv_headers(path, headers):
    with open(path, 'w') as file:
        file.write(f'{",".join(headers)}\n')


# there is probably a better way to have both csv and mysql without this much code duplication
def batch_process_csv(path, row_func, fact_headers, out_fpath):
    time0 = time.time()
    with open(path) as file:
        headers = next(file) # discard first row
        batch = deque([], maxlen=1000) # probably better separating the maxlen from the queue
        fact_headers = ('id',) + fact_headers
        i=0
        save_csv_headers(out_fpath,fact_headers)
        for line in file:
            row = split_line(line)
            fact_line = row_func(row) 
            fact_line = (str(i),) + fact_line
            i+=1
            batch.append(fact_line)
            if len(batch) == batch.maxlen:
                # prefer to handle this by catching a queue full exception
                save_batch_csv(batch, out_fpath)

        # save remaining items in the queue
        if not len(batch) == 0:
            save_batch_csv(batch, out_fpath)
    time1 = time.time()
    print(f'read handle and write took {time1-time0} seconds on file {os.path.basename(path)}.')


# def new_dim_dict_old(name, headers):
#     # this lowkey begs for being a class
#     # definitely dict_info and maybe this whole file
#     if name in dicts:
#         raise ValueError(f'Dict with this name already exists: {name}')
#     dicts[name] = {}
#     dict_maxids[name] = 0
#     dict_headers[name] = headers

# def ensure_dim_old(name, headers):
#     if name in dicts:
#         if name not in dict_headers:
#             raise "Mismanaged dim dictionaries: name in dicts but not in dict_headers"
#             # maybe only print error and don't raise
#         old_headers = dict_headers[name]
#         if headers != old_headers:
#             raise ValueError(f'New headers do not match old headers. Old: {old_headers}; new: {headers}')
#     else:
#         #name not in dicts -> add
#         new_dim_dict(name, headers)


# # temporary
# def ensure_dim(dim, _):
#     _ensure_dim_dict(dim)

# def ensure_dims(dims_list):
#     for dim in dims_list:
#         ensure_dim(dim[0], dim[1])