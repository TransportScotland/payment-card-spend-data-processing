import shared_funcs


# fact3_headers = ['perc_rail', 'pan_cnt', 'txn_cnt', 'txn_gbp_amt',  'time_id', 'merchant_id', 'jour_purpose_id']
def create_dims(row):
    """
    Transform a row of the File 3 CSV into a fact table row, also filling up dimensions.
    """

    # dimensions and measures (mixed this time because that's the order they were in)
    time_id = shared_funcs.handle_time(row)
    merchant_id = shared_funcs.handle_loc(row, 1, 2)
    # todo handle this differently
    if merchant_id == -1:
        raise
    perc_rail = row[3]
    jour_purpose_id = shared_funcs.handle_one_dim('purpose', row, 4)

    pan_cnt = row[5]
    txn_cnt = row[6]
    txn_gbp_amt = row[7]

    # return row tuple
    return (perc_rail, pan_cnt, txn_cnt, txn_gbp_amt, time_id, merchant_id, jour_purpose_id)


def etl(in_path, fact_table_name='fact3'):
    """
    Start the ETL process for File 3 of the card data.
    """

    # ensure relevant dimensions are set up
    shared_funcs.ensure_dim_dicts('time', 'location', 'purpose')
    # call create_dims on each row and save results into db
    shared_funcs.batch_process(in_path, create_dims, fact_table_name)


