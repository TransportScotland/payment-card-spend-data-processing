import shared_funcs


# fact_headers = ['perc_jour', 'perc_pan', 'time_id', 'merchant_id', 'transport_mode_id']
def create_dims(row):
    """
    Transform a row of the File 4 CSV into a fact table row, also filling up dimensions.
    """


    # dimensions and measures
    time_id = shared_funcs.handle_time(row)
    merchant_id = shared_funcs.handle_loc(row, 1, 2, 'POSTCODE_SECTOR')
    # todo handle this differently
    if merchant_id == -1:
        raise
    transport_mode_id = shared_funcs.handle_one_dim('transport_mode',row, 3)

    perc_jour = row[4]
    perc_pan = row[5]
    # return fact table row as tuple
    return (perc_jour, perc_pan, time_id, merchant_id, transport_mode_id)



def etl(in_path, fact_table_name = 'fact4'):
    """
    Start the ETL process for File 4 of the card data.
    """
    # shared_funcs.ensure_dim('time', ['raw', 'year', 'quarter', 'month', 'id'])
    # shared_funcs.ensure_dim('location',['sector', 'district', 'area', 'region', 'id'])
    # shared_funcs.ensure_dim('transport_mode', ['transport_mode', 'id'])
    # fact_list = []
    
    shared_funcs.ensure_dim_dicts('time', 'location', 'transport_mode')
    # time0 = time.time()
    shared_funcs.batch_process(in_path, create_dims, fact_table_name)
    # shared_funcs.batch_process(in_path, create_dims, fact_headers, out_fpath)
    # shared_funcs.read_and_handle(os.path.join(data_folder, file_name), create_dims, fact_list)
    # time1 = time.time()
    # print(f'read and handle took {time1-time0} seconds on File 4.')
    
    # return fact_list



