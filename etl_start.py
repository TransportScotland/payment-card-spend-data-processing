import pandas as pd
import time
pd.options.display.max_columns = 20

# this file is currently very messy.
# I will clean it up later, once I have most of the basic functionality down.


time_dict = {}
location_dict = {}
category_dict = {}
# time and category would prob be better as lists rather than dictionaries

dicts = {'time': time_dict, 'location': location_dict, 'category': category_dict}
dict_maxids = {'time' : 0, 'location' : 0, 'category': 0}

# pan_cnt, txn_cnt, txn_gbp_amt, time_id, cardholder_id, merchant_id, cat1_id, cat2_id, cat3_id
fact_list = [] 

skipped_rows_list = []

def add_to_dict_if_not_in(dict_name, key, values: list):
    if key not in dicts[dict_name]:
        values.append(dict_maxids[dict_name])
        dicts[dict_name][key] = values
        dict_maxids[dict_name] = dict_maxids[dict_name] + 1
        return values[-1]
    else:
        return dicts[dict_name][key][-1] # return id of new key

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

    time_id = add_to_dict_if_not_in('time', time_raw, [year, quarter, month])
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

# removing speeds up by 33%
def handle_categories(row, indices = slice(9, 12)):
    # cats = [row[f'mcc_rank{i+1}'] for i in range(3)]
    cats = row[indices]
    cat_ids = [add_to_dict_if_not_in('category', cat, [cat]) for cat in cats]
    return tuple(cat_ids)


def create_dims_f1(row_l):
    row = row_l
    time_id = handle_time(row)
    # time_id = 0 
    # cardholder_id = 0
    # merchant_id = 0
    cardholder_id = handle_loc(row, 2, 3)
    merchant_id = handle_loc(row, 4, 5)
    if cardholder_id == -1 or merchant_id == -1:
        skipped_rows_list.append(row)
        return
    # cat1_id, cat2_id, cat3_id = (0,0,0) 
    cat1_id, cat2_id, cat3_id = handle_categories(row)

    pan_cnt = row[6]
    txn_cnt = row[7]
    txn_gbp_amt = row[8]
    fact_list.append([pan_cnt, txn_cnt, txn_gbp_amt, time_id, cardholder_id, merchant_id, cat1_id, cat2_id, cat3_id])


def read_and_handle(path):
    with open(path) as file:
        headers = file.__next__().rstrip().split(',')[1:] #skipping col 0 for now
        print(headers)
        for line in file:
            vals = line.rstrip().split(',')[1:]
            create_dims_f1(vals)



time0 = time.time()
read_and_handle('data/file1.csv')
time1 = time.time()
print(f'read and handle took {time1-time0} seconds on the whole dataset')


category_df = pd.DataFrame(data= category_dict.values(), columns = ['category', 'id'])
location_df = pd.DataFrame(data = location_dict.values(), columns = ['sector', 'district', 'area', 'region', 'id'])
time_df = pd.DataFrame(data = [ [k] + v for k, v in time_dict.items()], columns=['raw', 'year', 'quarter', 'month', 'id'])
# time_df = pd.DataFrame(data = [ v.insert(0,k) for k, v in time_dict.items()], columns=['raw', 'year', 'quarter', 'month', 'id']) # faster but in-place
fact_df = pd.DataFrame(fact_list, columns=['pan_cnt', 'txn_cnt', 'txn_gbp_amt', 'time_id', 'cardholder_id', 'merchant_id', 'cat1_id', 'cat2_id', 'cat3_id'])

print(time_df)
print(location_df)
print(category_df)

print(fact_df)











# this should probably be a separate file but putting it here for now



fact3_list = []
purpose_dict = {}
dicts['purpose'] = purpose_dict
dict_maxids['purpose'] = 0


def handle_journey_purpose_l(row):
    jour_purpose = row[4]
    id = add_to_dict_if_not_in('purpose', jour_purpose, [jour_purpose])
    return id

# skipping all processing speeds up by ~70% (or a factor of 3-4)
def create_dims_f3_l(row):
    time_id = handle_time(row) # can keep
    
    merchant_id = handle_loc(row, 1, 2)
    if merchant_id == -1:
        # probably better throw an exception instead of -1. but check performance
        skipped_rows_list.append(row)
        return
    perc_rail = row[3]
    jour_purpose_id = handle_journey_purpose_l(row)

    pan_cnt = row[5]
    txn_cnt = row[6]
    txn_gbp_amt = row[7]
    fact3_list.append([perc_rail, pan_cnt, txn_cnt, txn_gbp_amt, time_id, merchant_id, jour_purpose_id])



def read_and_handle_file3(path):
    with open(path) as file:
        
        headers = file.__next__().rstrip().split(',')[1:] #skipping col 0 for now
        print(headers)
        for line in file:
            vals = line.rstrip().split(',')[1:]
            create_dims_f3_l(vals)



time0 = time.time()
read_and_handle_file3('data/file3.csv')
time1 = time.time()
print(f'read and handle of file 3 took {time1-time0} seconds.')


# print(f' fact3 list: {fact3_list}')
print(f'skipped rows: {skipped_rows_list}')

location_df = pd.DataFrame(data = location_dict.values(), columns = ['sector', 'district', 'area', 'region', 'id'])
print(location_df)


fact3_df = pd.DataFrame(fact3_list, columns=['perc_rail', 'pan_cnt', 'txn_cnt', 'txn_gbp_amt', 'time_id', 'merchant_id', 'jour_purpose_id'])

print(fact3_df)















# again this should likely be a different file. I'll separate them later


# File 2

fact2_list = []
m_channel_dict = {}
dicts['m_channel'] = m_channel_dict
dict_maxids['m_channel'] = 0

# a lotta code duplication 'ere innit
def handle_single_category(row, index = 3):
    cat = row[index]
    cat_id = add_to_dict_if_not_in('category', cat, [cat])
    return cat_id

def handle_merchant_channel(row, index = 5):
    mc = row[index]
    mc_id = add_to_dict_if_not_in('m_channel', mc, [mc])
    return mc_id

def create_dims_f2(row):
    time_id = handle_time(row) # can keep
    
    cardholder_id = handle_loc(row, 2, 3, 'Postcode_Sector')
    if cardholder_id == -1:
        # probably better throw an exception instead of -1. but check performance
        skipped_rows_list.append(row)
        return
    category_id = handle_single_category(row, 4)
    m_channel_id = handle_merchant_channel(row, 5)

    pan_cnt = row[6]
    txn_cnt = row[7]
    txn_gbp_amt = row[8]
    merchant_outlet_count = row[9]
    percent_repeat = row[10]
    fact2_list.append([pan_cnt, txn_cnt, txn_gbp_amt, merchant_outlet_count, percent_repeat, 
        time_id, cardholder_id, category_id, m_channel_id])


def read_and_handle_file2(path):
    with open(path) as file:
        
        headers = file.__next__().rstrip().split(',')[1:] #skipping col 0 for now
        print(headers)
        for line in file:
            vals = line.rstrip().split(',')[1:]
            create_dims_f2(vals)




time0 = time.time()
read_and_handle_file2('data/file2.csv')
time1 = time.time()
print(f'read and handle of file 2 took {time1-time0} seconds.')


print(f'skipped rows: {skipped_rows_list[:10]}, maybe more: size {len(skipped_rows_list)}')

location_df = pd.DataFrame(data = location_dict.values(), columns = ['sector', 'district', 'area', 'region', 'id'])
print(location_df)
print(m_channel_dict)
print(category_dict)

fact2_df = pd.DataFrame(fact2_list, columns=['pan_cnt', 'txn_cnt', 'txn_gbp_amt',
'merchant_outlet_count', 'percent_repeat', 
'time_id', 'cardholder_id', 'category_id', 'm_channel_id'])

print(fact2_df)












# for the last time this needs refactored but I'll do that later


# File4 


fact4_list = []
transport_mode_dict = {}
dicts['transport'] = transport_mode_dict
dict_maxids['transport'] = 0

def handle_transport_mode(row, index = 3):
    tm = row[index]
    tm_id = add_to_dict_if_not_in('transport', tm, [tm])
    return tm_id

def create_dims_f4(row):
    time_id = handle_time(row) # can keep
    
    # print(row)
    merchant_id = handle_loc(row, 1, 2, 'POSTCODE_SECTOR')
    if merchant_id == -1:
        # probably better throw an exception instead of -1. but check performance
        skipped_rows_list.append(row)
        return
    transport_mode_id = handle_transport_mode(row, 3)

    perc_jour = row[4]
    perc_pan = row[5]
    fact4_list.append([perc_jour, perc_pan, time_id, merchant_id, transport_mode_id])


#also code duplication and giving bad bugs. pass fn in as param
def read_and_handle_file4(path):
    with open(path) as file:
        
        headers = file.__next__().rstrip().split(',')[1:] #skipping col 0 for now
        print(headers)
        for line in file:
            vals = line.rstrip().split(',')[1:]
            create_dims_f4(vals)




time0 = time.time()
read_and_handle_file4('data/file4.csv')
time1 = time.time()
print(f'read and handle of file 4 took {time1-time0} seconds.')


print(f'skipped rows: {skipped_rows_list[:10]}, maybe more: size {len(skipped_rows_list)}')

location_df = pd.DataFrame(data = location_dict.values(), columns = ['sector', 'district', 'area', 'region', 'id'])
print(location_df)
print(m_channel_dict)
print(category_dict)

fact4_df = pd.DataFrame(fact4_list, columns=['perc_jour', 'perc_pan',
'time_id', 'merchant_id', 'transport_mode_id'])

print(fact4_df)


folder  = 'wh/'
def dimdfs_to_csv():
    time_df = pd.DataFrame(data = [ [k] + v for k, v in time_dict.items()], columns=['raw', 'year', 'quarter', 'month', 'id'])
    location_df = pd.DataFrame(data = location_dict.values(), columns = ['sector', 'district', 'area', 'region', 'id'])
    category_df = pd.DataFrame(data= category_dict.values(), columns = ['category', 'id'])
    purpose_df = pd.DataFrame(data= purpose_dict.values(), columns=['purpose', 'id'])
    m_channel_df = pd.DataFrame(data= m_channel_dict.values(), columns=['channel', 'id'])
    transport_df = pd.DataFrame(data= transport_mode_dict.values(), columns=['transport_mode', 'id'])

    dflist = [time_df, location_df, category_df, purpose_df, m_channel_df, transport_df]
    dfnames = ['time', 'location', 'category', 'purpose', 'm_channel', 'transport']
    for df, name in zip(dflist, dfnames):
        df.to_csv(folder + name + '.csv')

print(dicts.keys())
dimdfs_to_csv()

fact_df.to_csv(folder + 'fact1.csv')
fact2_df.to_csv(folder + 'fact2.csv')
fact3_df.to_csv(folder + 'fact3.csv')
fact4_df.to_csv(folder + 'fact4.csv')


