# Generate random data according to the structure of the sample files
# for until we are delivered the actual data
#
# author: Transport Scotland

import numpy as np
import pandas as pd
import os

columns_f1_str = 'time_frame,cardholder_type,cardholder_location_level,cardholder_location,merchant_location_level,merchant_location,pan_cnt,txn_cnt,txn_gbp_amt,mcc_rank1,mcc_rank2,mcc_rank3'
columns_f2_str = 'time_frame,cardholder_type,cardholder_postcode_level,cardholder_location,mcc,merchant_channel,pan_cnt,txn_cnt,txn_gbp_amt,merchant_outlet_cnt,percent_repeat'
columns_f3_str = 'time_frame,merchant_location_level,merchant_location,perc_rail_users,jour_purpose,pan_cnt,txn_cnt,txn_gbp_amt,Day_of_week'
columns_f4_str = 'time_frame,merchant_location_level,merchant_location,Transport_mode,perc_jour,perc_pan'

columns_f1 = columns_f1_str.split(',')
columns_f2 = columns_f2_str.split(',')
columns_f3 = columns_f3_str.split(',')
columns_f4 = columns_f4_str.split(',')


def fill_rows(df, num: int, values: dict):
    for k, v in values.items():
        print(f'k: {k}, v: {v}')
        df[k] = np.full(num, v)

def rand_postcode_sector_column(num):
    # generating only a small choice to limit number of options

    # area = np.random.choice(['G', 'EH'], num )
    # limiting tne number of different area options 
    # in order to have any matching origin-destination pairs in smaller data
    area = np.random.choice(['AB', 'DD', 'DG', 'EH', 'FK', 'G', 'HS', 'IV'   ], num)#   , 'KA', 'KY', 'ML', 'PA', 'PH', 'TD'],num)
    district = np.random.randint(1, 16, size= num).astype(str)
    sector = np.random.randint(0, 10, size = num).astype(str)

    # return np.char.join('', [area, district, ' ', sector])
    a = np.char.add(area, district)
    b = np.char.add(a, ' ')
    c = np.char.add(b, sector)
    return c

def gen_file1(num_rows = 10):

    df = pd.DataFrame(columns=columns_f1)
    fill_rows(df, num_rows, {
        'cardholder_type': 'Domestic',
        'cardholder_location_level': 'POSTCODE_SECTOR',
        'merchant_location_level': 'POSTCODE_SECTOR',
        'mcc_rank1':  'Grocery Stores/Supermarkets - 5411',
        'mcc_rank2':  'Misc Food Stores - Default - 5499',
        'mcc_rank3':  'Restaurants - 5812',
    })

    df['time_frame'] = np.random.randint(202201, 202204, num_rows).astype(str)
    df['cardholder_location'] = rand_postcode_sector_column(num_rows)
    df['merchant_location'] = rand_postcode_sector_column(num_rows)
    df['pan_cnt'] = np.random.randint(15, 100, num_rows)
    df['txn_cnt'] = df['pan_cnt'] * (np.random.uniform(1,3, num_rows))
    df['txn_cnt'] = df['txn_cnt'].astype(int)
    df['txn_gbp_amt'] = np.random.uniform(300, 3000, num_rows)


    # set 10% samples to have same cardholder and merchant - for showing up in visualisations
    # remove if actual randomness desired
    df.loc[df['cardholder_location'].sample(int(num_rows/10)).index,
        'merchant_location'] = df['cardholder_location']
    
    # set random selection of sectors to actually be just districts
    rand_index = df.sample(int(num_rows/20)).index
    df.loc[rand_index, 'merchant_location'] = df['merchant_location'].str[:-2]
    df.loc[rand_index, 'merchant_location_level'] = 'POSTCODE_DISTRICT' 
    return df

def gen_file2(num_rows = 10):

    df = pd.DataFrame(columns=columns_f2)
    fill_rows(df, num_rows, {
        'cardholder_type': 'Domestic',
        'cardholder_postcode_level': 'Postcode_Sector',
        'mcc': 'Antique Reproduction Stores - 5937',
        'merchant_channel': 'Face to Face',
    })

    df['time_frame'] = np.random.randint(202201, 202204, num_rows).astype(str)
    df['cardholder_location'] = np.core.defchararray.add('EH2 ', np.random.randint(0, 10, num_rows).astype(str))
    df['pan_cnt'] = np.random.randint(10, 10000, num_rows)
    df['txn_cnt'] = df['pan_cnt'] * (np.random.uniform(1,2, num_rows))
    df['txn_cnt'] = df['txn_cnt'].astype(int)
    df['txn_gbp_amt'] = np.random.uniform(1000, 1e6, num_rows)
    df['merchant_outlet_cnt'] = np.random.randint(10, 1000, num_rows)
    df['percent_repeat'] = np.random.normal(0.5, 0.1, num_rows)
    return df

def gen_file3(num_rows = 10):

    df = pd.DataFrame(columns=columns_f3)
    fill_rows(df, num_rows, {
        'merchant_location_level': 'POSTCODE_SECTOR',
    })

    df['time_frame'] = np.random.randint(202201, 202204, num_rows).astype(str)
    df['merchant_location'] = np.core.defchararray.add('G13 ', np.random.randint(0, 10, num_rows).astype(str))
    df['perc_rail_users'] = np.random.random_sample(num_rows)
    df['jour_purpose'] = np.random.choice(['WINE & DINE', 'SHOPPING SPREE', 'OTHERS', 'ENTERTAINMENT'], num_rows)
    df['pan_cnt'] = np.random.randint(5000, 110000, num_rows)
    df['txn_cnt'] = df['pan_cnt'] * (np.random.uniform(1,3, num_rows))
    df['txn_cnt'] = df['txn_cnt'].astype(int)
    df['txn_gbp_amt'] = np.random.uniform(1e5, 5e6, num_rows)
    df['Day_of_week'] = np.random.choice(['WEEKDAY', 'SATURDAY', 'SUNDAY'], num_rows)
    return df

def gen_file4(num_rows = 10):

    df = pd.DataFrame(columns=columns_f4)
    fill_rows(df, num_rows, {
        'merchant_location_level': 'POSTCODE_SECTOR',
    })

    df['time_frame'] = np.random.randint(202201, 202204, num_rows).astype(str)
    df['merchant_location'] = np.core.defchararray.add('AB3 ', np.random.randint(0, 10, num_rows).astype(str))
    df['Transport_mode'] = np.random.choice(['BUS', 'CAR', 'RAIL'], num_rows, [0.2, 0.3, 0.5])
    df['perc_jour'] = np.random.random_sample(num_rows)
    df['perc_pan'] = np.random.random_sample(num_rows)

    return df


pd.options.display.max_columns = 20 # let pandas display more columns
np.random.seed(0) # setting the seed for some reproducibility later
file1 = gen_file1(int(1e4))
# print(file1)

np.random.seed(0)
file2 = gen_file2(1900)
# print(file2)

np.random.seed(0)
file3 = gen_file3(1800)
# print(file3)

np.random.seed(0)
file4 = gen_file4(1700)
# print(file4)

os.makedirs('data', exist_ok=True)
file1.to_csv('data/file1_pa_1e4.csv', index=False)
file2.to_csv('data/file2.csv', index=False)
file3.to_csv('data/file3.csv', index=False)
file4.to_csv('data/file4.csv', index=False)

