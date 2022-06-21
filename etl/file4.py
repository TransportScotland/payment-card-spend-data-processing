import shared_funcs
import os
import time

fact_headers = ['perc_jour', 'perc_pan', 
    'time_id', 'merchant_id', 'transport_mode_id']


# def create_dims(row, fact_list):
def create_dims(row):
    time_id = shared_funcs.handle_time(row) # can keep
    
    # print(row)
    merchant_id = shared_funcs.handle_loc(row, 1, 2, 'POSTCODE_SECTOR')
    if merchant_id == -1:
        # probably better throw an exception instead of -1. but check performance
        raise
    transport_mode_id = shared_funcs.handle_one('transport_mode',row, 3)

    perc_jour = row[4]
    perc_pan = row[5]
    return (perc_jour, perc_pan, time_id, merchant_id, transport_mode_id)
    # fact_list.append([perc_jour, perc_pan, time_id, merchant_id, transport_mode_id])



def etl(in_path, out_fpath):
    shared_funcs.ensure_dim('time', ['raw', 'year', 'quarter', 'month', 'id'])
    shared_funcs.ensure_dim('location',['sector', 'district', 'area', 'region', 'id'])
    shared_funcs.ensure_dim('transport_mode', ['transport_mode', 'id'])
    fact_list = []
    
    # time0 = time.time()
    shared_funcs.batch_process(in_path, create_dims, fact_headers, out_fpath)
    # shared_funcs.read_and_handle(os.path.join(data_folder, file_name), create_dims, fact_list)
    # time1 = time.time()
    # print(f'read and handle took {time1-time0} seconds on File 4.')
    
    return fact_list



