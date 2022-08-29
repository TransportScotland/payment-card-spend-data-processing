
from dimensions import *
from shared_funcs import *

def convert_row(split, census_dict) -> list:
    # time_frame	cardholder_type	cardholder_location_level	
    # cardholder_location	mcc	    merchant_channel	
    # pan_cnt	txn_cnt	txn_gbp_amt	merchant_outlet_cnt	percent_repeat


    time_dim = time_cols(split[0])
    cardholder_location = location_cols(split[3], split[2])
    if isinstance(cardholder_location, MismatchedLocationLevel):
        split[2] = cardholder_location.got_level
        cardholder_location = cardholder_location.return_anyway
    cardholder_census_info = census_info_col(census_dict, split[3])
    return [
        *split, 
        *time_dim, 
        *cardholder_location,
        *cardholder_census_info,
    ]
    
db_creation_string_columns = str(
            "time_frame String NOT NULL,"
            "cardholder_type String NOT NULL,"
            "cardholder_location_level String NOT NULL,"
            "cardholder_location String NOT NULL,"
            "mcc String NOT NULL,"
            "merchant_channel String NOT NULL,"
            "pan_cnt UInt32 NOT NULL,"
            "txn_cnt UInt64 NOT NULL,"
            "txn_gbp_amt Float32 NOT NULL,"
            "merchant_outlet_cnt UInt32 NOT NULL,"
            "percent_repeat Float32 NOT NULL,"
            'month Nullable(Int8),'
            'quarter Int8,'
            'year Int16,'
            'cardholder_postal_sector Nullable(String),'
            'cardholder_postal_district Nullable(String),'
            'cardholder_postal_area Nullable(String),'
            'cardholder_postal_town Nullable(String),'
            'cardholder_postal_region Nullable(String),'
            'cardholder_population Nullable(UInt32),'
            'cardholder_centre_lat Nullable(Float32),'
            'cardholder_centre_lng Nullable(Float32)'
            ") ENGINE = MergeTree() "
            "ORDER BY (time_frame, cardholder_location, mcc)"
)


passwords_dict['module2_sample_10k.zip']= None
passwords_dict['nr_module_2_2020.csv.zip']= b'NetworkRail_2020'
passwords_dict['nr_module_2_2021.csv.zip']= b'NetworkRail_2021'
passwords_dict['nr_module_2_2022.csv.zip']= b'NetworkRail_2022'
    
zip_filenames_dict['module2_sample_10k.zip']= 'module2_sample_10k.csv'
zip_filenames_dict['nr_module_2_2020.csv.zip']= 'san-ssapfs/edge/home/chaudhup/Network_Rail/nr_module_2_2020.csv'
zip_filenames_dict['nr_module_2_2021.csv.zip']= 'san-ssapfs/edge/home/chaudhup/Network_Rail/nr_module_2_2021.csv'
zip_filenames_dict['nr_module_2_2022.csv.zip']= 'san-ssapfs/edge/home/chaudhup/Network_Rail/nr_module_2_2022.csv'


def etl_sample_file():
    return etl(['data/module2_sample_10k.zip'], table_name='module2_sample')

def etl_real_files():
    return etl([
        '/mnt/sftp/in/nr_module_2_2021.csv.zip',
        '/mnt/sftp/in/nr_module_2_2020.csv.zip',
        '/mnt/sftp/in/nr_module_2_2022.csv.zip',
        ], table_name = 'module2')

def etl(infpaths, table_name = 'module2'):
    import time
    time0= time.perf_counter()
    census_dict = load_census_dict()
    print(f'Loading census dict took {time.perf_counter() - time0}s.')

    dbinfo = DBInfo(db_creation_string_columns, table_name = table_name)
    apply_and_save(infpaths, convert_row, dbinfo, census_dict)

if __name__ == '__main__':
    import time
    time0 = time.perf_counter()

    etl_sample_file()

    print(f'Took {time.perf_counter() - time0} seconds to process {line_num} lines.')