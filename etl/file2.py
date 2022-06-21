import shared_funcs
import os
import time

fact_headers = ['pan_cnt', 'txn_cnt', 'txn_gbp_amt',
'merchant_outlet_count', 'percent_repeat', 
'time_id', 'cardholder_id', 'category_id', 'm_channel_id']


# def create_dims(row, fact_list):
def create_dims(row):
    time_id = shared_funcs.handle_time(row) # can keep
    
    cardholder_id = shared_funcs.handle_loc(row, 2, 3, 'Postcode_Sector')
    if cardholder_id == -1:
        # probably better throw an exception above instead of returning -1
        # but return an error code rather than raise out of here bc performance
        raise
    category_id = shared_funcs.handle_one('category',row, 4)
    m_channel_id = shared_funcs.handle_one('m_channel',row, 5)

    pan_cnt = row[6]
    txn_cnt = row[7]
    txn_gbp_amt = row[8]
    merchant_outlet_count = row[9]
    percent_repeat = row[10]
    return (pan_cnt, txn_cnt, txn_gbp_amt, merchant_outlet_count, percent_repeat, 
        time_id, cardholder_id, category_id, m_channel_id)
    # fact_list.append([pan_cnt, txn_cnt, txn_gbp_amt, merchant_outlet_count, percent_repeat, 
    #     time_id, cardholder_id, category_id, m_channel_id])

def etl(in_path, out_fpath):
    shared_funcs.ensure_dim('time', ['raw', 'year', 'quarter', 'month', 'id'])
    shared_funcs.ensure_dim('location',['sector', 'district', 'area', 'region', 'id'])
    shared_funcs.ensure_dim('category', ['category', 'id'])
    shared_funcs.ensure_dim('m_channel', ['channel', 'id'])
    fact_list = []
    
    # time0 = time.time()
    shared_funcs.batch_process(in_path, create_dims, fact_headers, out_fpath)
    # shared_funcs.read_and_handle(os.path.join(data_folder, file_name), create_dims, fact_list)
    # time1 = time.time()
    # print(f'read and handle took {time1-time0} seconds on File 2.')

    return fact_list



