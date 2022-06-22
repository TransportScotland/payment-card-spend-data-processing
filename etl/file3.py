import shared_funcs

# fact_headers = ['perc_rail', 'pan_cnt', 'txn_cnt', 'txn_gbp_amt', 
# 'time_id', 'merchant_id', 'jour_purpose_id']


# def create_dims(row, fact_list):
def create_dims(row):
    time_id = shared_funcs.handle_time(row) # can keep
    
    merchant_id = shared_funcs.handle_loc(row, 1, 2)
    if merchant_id == -1:
        # probably better throw an exception instead of -1. but check performance
        raise
    perc_rail = row[3]
    jour_purpose_id = shared_funcs.handle_one_dim('purpose', row, 4)

    pan_cnt = row[5]
    txn_cnt = row[6]
    txn_gbp_amt = row[7]
    return (perc_rail, pan_cnt, txn_cnt, txn_gbp_amt, time_id, merchant_id, jour_purpose_id)
    # fact_list.append([perc_rail, pan_cnt, txn_cnt, txn_gbp_amt, time_id, merchant_id, jour_purpose_id])


def etl(in_path, fact_table_name='fact3'):
    # shared_funcs.ensure_dim('time', ['raw', 'year', 'quarter', 'month', 'id'])
    # shared_funcs.ensure_dim('location',['sector', 'district', 'area', 'region', 'id'])
    # shared_funcs.ensure_dim('purpose', ['purpose', 'id'])
    # fact_list = []

    shared_funcs.ensure_dim_dicts('time', 'location', 'purpose')
    
    # time0 = time.time()
    # shared_funcs.read_and_handle(os.path.join(data_folder, file_name), create_dims, fact_list)
    # shared_funcs.batch_process(in_path, create_dims, fact_headers, out_fpath)
    shared_funcs.batch_process(in_path, create_dims, fact_table_name)

    # time1 = time.time()
    # print(f'read and handle took {time1-time0} seconds on File 3.')

    # return fact_list



