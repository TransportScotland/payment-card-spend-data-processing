import shared_funcs
import os
import numpy as np
import pandas as pd
import dask.dataframe as dd
import time

fact_headers = ['pan_cnt', 'txn_cnt', 'txn_gbp_amt', 'time_id', 'cardholder_id', 'merchant_id', 'cat1_id', 'cat2_id', 'cat3_id']

# removing speeds up by 33%
# def handle_categories(row, indices = slice(9, 12)):
#     # cats = [row[f'mcc_rank{i+1}'] for i in range(3)]
#     cats = row[indices]
#     cat_ids = [shared_funcs.add_to_dict_if_not_in('category', cat, [cat]) for cat in cats]
#     return tuple(cat_ids)


def create_dims(row, fact_list):
    time_id = shared_funcs.handle_time(row)
    # time_id = 0 
    # cardholder_id = 0
    # merchant_id = 0
    cardholder_id = shared_funcs.handle_loc(row, 2, 3)
    merchant_id = shared_funcs.handle_loc(row, 4, 5)
    if cardholder_id == -1 or merchant_id == -1:
        raise
    # cat1_id, cat2_id, cat3_id = (0,0,0) 
    # cat1_id, cat2_id, cat3_id = handle_categories(row)
    cat1_id = shared_funcs.handle_one('category', row, 9)
    cat2_id = shared_funcs.handle_one('category', row, 10)
    cat3_id = shared_funcs.handle_one('category', row, 11)

    pan_cnt = row[6]
    txn_cnt = row[7]
    txn_gbp_amt = row[8]
    return [pan_cnt, txn_cnt, txn_gbp_amt, time_id, cardholder_id, merchant_id, cat1_id, cat2_id, cat3_id]

# def create_dims_dd(row):


def etl(data_folder, file_name):

    pass
    # # instead prob do a dimensions list and create dim if not exists
    # shared_funcs.ensure_dim('time', ['raw', 'year', 'quarter', 'month', 'id'])
    # shared_funcs.ensure_dim('location',['sector', 'district', 'area', 'region', 'id'])
    # shared_funcs.ensure_dim('category', ['category', 'id'])
    # fact_list = []

    # import numba
    # aaa = numba.jit(create_dims)

    # time0 = time.time()
    # shared_funcs.read_and_handle(os.path.join(data_folder, file_name), aaa, fact_list)
    # time1 = time.time()
    # print(f'read and handle took {time1-time0} seconds on File 1.')

    # file1_dd = dd.read_csv(os.path.join(data_folder, file_name))
    # # print(file1_dd)
    # fact_list = []
    # fact_headers = ['pan_cnt', 'txn_cnt', 'txn_gbp_amt', 'time_id', 'cardholder_id', 'merchant_id', 'cat1_id', 'cat2_id', 'cat3_id']
    # metadf = pd.DataFrame(columns =fact_headers)
    # metadf.pan_cnt = metadf.pan_cnt.astype(np.int_)
    # metadf.txn_cnt = metadf.txn_cnt.astype(np.int_)
    # metadf.txn_gbp_amt = metadf.txn_gbp_amt.astype(np.double)
    # metadf.time_id = metadf.time_id.astype(np.int_)
    # metadf.cardholder_id = metadf.cardholder_id.astype(np.int_)
    # metadf.merchant_id = metadf.merchant_id.astype(np.int_)
    # metadf.cat1_id = metadf.cat1_id.astype(np.int_)
    # metadf.cat2_id = metadf.cat2_id.astype(np.int_)
    # metadf.cat3_id = metadf.cat3_id.astype(np.int_)
    # # fact_dd = dd.from_pandas(fact1_df, npartitions= 10)
    # file1_dd.apply(create_dims, axis=1, args = (fact_list), meta=metadf)

    # res = file1_dd.compute()
    # print(res)
    # print(fact_list)

    # return fact_list


if __name__ == '__main__':
    etl('data/', 'file1.csv')