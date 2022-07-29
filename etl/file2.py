import shared_funcs

# Refer to file1.py for more detailed comments

# fact2_headers = ['pan_cnt', 'txn_cnt', 'txn_gbp_amt', 'merchant_outlet_count', 
#     'percent_repeat', 'time_id', 'cardholder_id', 'category_id', 'm_channel_id']

def create_dims(row):
    """
    Transform a row of the File 2 CSV into a fact table row, also filling up dimensions.
    """

    # dimensions
    time_id = shared_funcs.handle_time(row)
    cardholder_id = shared_funcs.handle_loc(row, 2, 3, 'Postcode_Sector') # File1 had full uppercase, this one Title_Snake case
    
    # todo handle this differently
    if cardholder_id == -1:
        raise
    category_id = shared_funcs.handle_one_dim('category',row, 4)
    m_channel_id = shared_funcs.handle_one_dim('m_channel',row, 5)

    # measures
    pan_cnt = row[6]
    txn_cnt = row[7]
    txn_gbp_amt = row[8]
    merchant_outlet_count = row[9]
    percent_repeat = row[10]

    # return ordered tuple
    return (pan_cnt, txn_cnt, txn_gbp_amt, merchant_outlet_count, percent_repeat, 
        time_id, cardholder_id, category_id, m_channel_id)


def etl(in_path, fact_table_name = 'fact2'):
    """
    Start the ETL process for File 2 of the card data
    """

    # ensure relevant dimensions are set up
    shared_funcs.ensure_dim_dicts('time', 'location', 'category', 'm_channel')
    # start processing, calling create_dims on each row
    shared_funcs.batch_process(in_path, create_dims, fact_table_name)



