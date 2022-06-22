import shared_funcs

# fact_headers = ('pan_cnt', 'txn_cnt', 'txn_gbp_amt', 'time_id', 'cardholder_id', 'merchant_id', 'cat1_id', 'cat2_id', 'cat3_id')

# removing speeds up by 33%
# def handle_categories(row, indices = slice(9, 12)):
#     # cats = [row[f'mcc_rank{i+1}'] for i in range(3)]
#     cats = row[indices]
#     cat_ids = [shared_funcs.add_to_dict_if_not_in('category', cat, [cat]) for cat in cats]
#     return tuple(cat_ids)


# def create_dims(row, fact_list):
def create_dims(row):
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
    cat1_id = shared_funcs.handle_one_dim('category', row, 9)
    cat2_id = shared_funcs.handle_one_dim('category', row, 10)
    cat3_id = shared_funcs.handle_one_dim('category', row, 11)

    pan_cnt = row[6]
    txn_cnt = row[7]
    txn_gbp_amt = row[8]
    return (pan_cnt, txn_cnt, txn_gbp_amt, time_id, cardholder_id, merchant_id, cat1_id, cat2_id, cat3_id)
    # fact_list.append([pan_cnt, txn_cnt, txn_gbp_amt, time_id, cardholder_id, merchant_id, cat1_id, cat2_id, cat3_id])



def etl(in_path, fact_table_name = 'fact1'):
    # db = shared_funcs.connect_to_db()

    # instead prob do a dimensions list and create dim if not exists
    # shared_funcs.ensure_dim('time', ['raw', 'year', 'quarter', 'month', 'id'])
    # shared_funcs.ensure_dim('location',['sector', 'district', 'area', 'region', 'id'])
    # shared_funcs.ensure_dim('category', ['category', 'id'])
    # fact_list = []

    # time0 = time.time()
    shared_funcs.ensure_dim_dicts('time', 'location', 'category')
    shared_funcs.batch_process(in_path, create_dims, fact_table_name)
    # shared_funcs.batch_process_csv(in_path, create_dims, fact_headers, out_fpath= fact_table_name)
    # shared_funcs.read_and_handle(os.path.join(data_folder, file_name), create_dims, fact_list)
    # time1 = time.time()
    # print(f'read and handle took {time1-time0} seconds on File 1.')

    # return fact_list


