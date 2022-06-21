import pandas as pd
import os
import time
import csv
# import queue # adds a TON of overhead, only for multiprocessing
from collections import deque


dicts = {}
dict_maxids = {}
dict_headers = {}

skipped_rows = []

def new_dim_dict(name, headers):
    # this lowkey begs for being a class
    # definitely dict_info and maybe this whole file
    if name in dicts:
        raise ValueError(f'Dict with this name already exists: {name}')
    dicts[name] = {}
    dict_maxids[name] = 0
    dict_headers[name] = headers

def add_to_dict_if_not_in(dict_name, key, values: list):
    if key not in dicts[dict_name]:
        values.append(dict_maxids[dict_name])
        dicts[dict_name][key] = values
        dict_maxids[dict_name] = dict_maxids[dict_name] + 1
        return values[-1]
    else:
        return dicts[dict_name][key][-1] # return id of new key

def handle_one(dict_name, row, row_index):
    val = row[row_index]
    id = add_to_dict_if_not_in(dict_name, val, [val])
    return id

# this is somewhat slow too, could prob be sped up
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

def ensure_dim(name, headers):
    if name in dicts:
        if name not in dict_headers:
            raise "Mismanaged dim dictionaries: name in dicts but not in dict_headers"
            # maybe only print error and don't raise
        old_headers = dict_headers[name]
        if headers != old_headers:
            raise ValueError(f'New headers do not match old headers. Old: {old_headers}; new: {headers}')
    else:
        #name not in dicts -> add
        new_dim_dict(name, headers)

# def ensure_dims(dims_list):
#     for dim in dims_list:
#         ensure_dim(dim[0], dim[1])

def split_line(line):
    return line.rstrip().split(',')

def save_batch_csv(batch, path):
    def row_to_saveable(row):
        return ','.join([str(e) for e in row]) + '\n'
    # save_lines = [','.join(str(batch.get_nowait())) for _ in range(batch.qsize())]
    # save_lines = [','.join(row) for row in batch]
    # save_str = '\n'.join(save_lines)
    # save_lines = [row_to_saveable(batch.popleft()) for _ in range(len(batch))]
    save_lines = [row_to_saveable(e) for e in batch]
    # save_lines = [row_to_saveable(batch.get_nowait()) for _ in range(batch.qsize())]
    # print(save_lines)
    with open(path, 'a') as file:
        #probably instead open the file/connection at the beginning and close at the end
        # file.write(save_str)
        file.writelines(save_lines)

def save_csv_headers(path, headers):
    with open(path, 'w') as file:
        file.write(f'{",".join(headers)}\n')

def batch_process(path, row_func, fact_headers, out_fpath):
    time0 = time.time()
    with open(path) as file:
        headers = next(file)
        # batch = queue.Queue(2000)
        batch = deque([], 2000)

        save_csv_headers(out_fpath,fact_headers)
        for line in file:
            row = split_line(line)
            fact_line = row_func(row)
            try:
                # batch.put_nowait(fact_line)
                batch.append(fact_line)
            # except queue.Full:
            except IndexError:
                save_batch_csv(batch, out_fpath)

        # save remaining items in the queue
        # if not batch.empty():
        if not len(batch) == 0:
            save_batch_csv(batch, out_fpath)
            # if batch.full():
            #     save_batch_csv(batch)
    time1 = time.time()
    print(f'read handle and write took {time1-time0} seconds on file {os.path.basename(path)}.')
    



def get_headers(dict_name):
    return dict_headers[dict_name]

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
    