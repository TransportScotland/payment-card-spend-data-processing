import pandas as pd
import os


dicts = {}
dict_maxids = {}
dict_headers = {}

def new_dim_dict(name, headers):
    # this lowkey begs for being a class
    # definitely dict_info and maybe this whole file
    if name in dicts:
        raise ValueError(f'Dict with this name already exists: {name}')
    dicts[name] = {}
    dict_maxids[name] = 0
    dict_headers[name] = headers
    pass

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
    # won't work right - postcodes can be weird, eg AA1A 1AA

    district = sector.rsplit(' ', maxsplit=1)[0]

    # first digit if second digit is number, else first two digits.
    # to confirm if any postcodes areas are 3 chars (simple loop then)
    area = district[0] if district[1].isnumeric() else district[0:2]

    # region might have to be done differently (current only known one is scotland)
    region = 'unknown'

    return [sector, district, area, region]

# removing increases speed by 15%
def handle_time(row, index = 0):
    time_raw = str(row[index]) #optional depending on stuff
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



def read_and_handle(path, row_func, fact_list):
    with open(path) as file:
        headers = file.__next__().rstrip().split(',')[1:] #skipping col 0 for now
        # print(headers)
        for line in file:
            vals = line.rstrip().split(',')[1:]
            row_func(vals, fact_list)

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
    df.to_csv(os.path.join(folder, name))