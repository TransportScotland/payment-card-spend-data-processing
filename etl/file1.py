import shared_funcs
import os
import time

fact_headers = ['pan_cnt', 'txn_cnt', 'txn_gbp_amt', 'time_id', 'cardholder_id', 'merchant_id', 'cat1_id', 'cat2_id', 'cat3_id']
skipped_rows_list = []

# removing speeds up by 33%
def handle_categories(row, indices = slice(9, 12)):
    # cats = [row[f'mcc_rank{i+1}'] for i in range(3)]
    cats = row[indices]
    cat_ids = [shared_funcs.add_to_dict_if_not_in('category', cat, [cat]) for cat in cats]
    return tuple(cat_ids)


def create_dims(row, fact_list):
    time_id = shared_funcs.handle_time(row)
    # time_id = 0 
    # cardholder_id = 0
    # merchant_id = 0
    cardholder_id = shared_funcs.handle_loc(row, 2, 3)
    merchant_id = shared_funcs.handle_loc(row, 4, 5)
    if cardholder_id == -1 or merchant_id == -1:
        skipped_rows_list.append(row)
        return
    # cat1_id, cat2_id, cat3_id = (0,0,0) 
    cat1_id, cat2_id, cat3_id = handle_categories(row)

    pan_cnt = row[6]
    txn_cnt = row[7]
    txn_gbp_amt = row[8]
    fact_list.append([pan_cnt, txn_cnt, txn_gbp_amt, time_id, cardholder_id, merchant_id, cat1_id, cat2_id, cat3_id])


def etl(data_folder, file_name):
    shared_funcs.new_dim_dict('time', ['raw', 'year', 'quarter', 'month', 'id'])
    shared_funcs.new_dim_dict('location',['sector', 'district', 'area', 'region', 'id'])
    shared_funcs.new_dim_dict('category', ['category', 'id'])
    fact_list = []

    time0 = time.time()
    shared_funcs.read_and_handle(os.path.join(data_folder, file_name), create_dims, fact_list)
    time1 = time.time()

    print(f'read and handle took {time1-time0} seconds on File 1.')
    if skipped_rows_list:
        print(skipped_rows_list)
    return fact_list


