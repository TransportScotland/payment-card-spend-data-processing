import shared_funcs


# fact1_headers = ('pan_cnt', 'txn_cnt', 'txn_gbp_amt', 'time_id', 'cardholder_id', 'merchant_id', 'cat1_id', 'cat2_id', 'cat3_id')
def create_dims(row):
    """
    Turn a row tuple from the File 1 csv into a fact table entry, 
    filling up intermediate dimension tables along the way.

    Parameters:
        row: tuple/list indexable by numerical position in the File1 CSV file

    Returns:
        A row of the fact table as a tuple. Also may modify the dimension data structures
    """
    # add dimension data to dimension data structures if not already in, and get the relevant ID
    time_id = shared_funcs.handle_time(row)
    cardholder_id = shared_funcs.handle_loc(row, 2, 3)
    merchant_id = shared_funcs.handle_loc(row, 4, 5)

    # todo handle this differently
    # loc returns -1 if something else than POSTCODE_SECTOR was in the row.
    # currently raising an exception to skip this row - not the right way to do things.
    if cardholder_id == -1 or merchant_id == -1:
        raise
    
    # get measures straight from the row list
    pan_cnt = row[6]
    txn_cnt = row[7]
    txn_gbp_amt = row[8]

    # dimensions again
    # equivalent to: cat1_id, cat2_id, cat3_id = (shared_funcs.handle_one_dim('category', row, i) for i in [9, 10, 11])
    cat1_id = shared_funcs.handle_one_dim('category', row, 9)
    cat2_id = shared_funcs.handle_one_dim('category', row, 10)
    cat3_id = shared_funcs.handle_one_dim('category', row, 11)

    
    # return a tuple of the data to be saved into the fact table.
    # currently this must be in this order because that is how it is set up in the database
    # todo consider turning this into a class/struct with the named variables and a function like get_ordered_tuple()
    return (pan_cnt, txn_cnt, txn_gbp_amt, time_id, cardholder_id, merchant_id, cat1_id, cat2_id, cat3_id)


def etl(in_path, fact_table_name = 'fact1'):
    """
    Starts the ETL process of File 1 of the card data.

    Parameters:
        in_path: File path to the input CSV file
        fact_table_name: (optional) name of the fact table in the database. (May not work correctly)
    """

    # ensure dimensions are set up and ready to accept data from this file
    shared_funcs.ensure_dim_dicts('time', 'location', 'category')

    # process the file, calling create_dims() on each row
    shared_funcs.batch_process(in_path, create_dims, fact_table_name)



