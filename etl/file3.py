
from dimensions import *
from shared_funcs import *

def convert_row(split, census_dict) -> list:

    # time_period	merchant_location_level	merchant_location	
    # perc_cards_rail	journey_purpose_level	journey_purpose	
    # pan_cnt	journeys	txn_gbp_amt	txn_cnt	day_of_week



    time_dim = time_cols(split[0])
    merchant_location = location_cols(split[2], split[1])
    if isinstance(merchant_location, MismatchedLocationLevel):
        split[1] = merchant_location.got_level
        merchant_location = merchant_location.return_anyway
    merchant_census_info = census_info_col(census_dict, split[5])
    return [
        *split, 
        *time_dim, 
        *merchant_location,
        *merchant_census_info,
    ]
    
db_creation_string_columns = str(
            "time_frame String NOT NULL,"
            "merchant_location_level String NOT NULL,"
            "merchant_location String NOT NULL,"
            "perc_cards_rail Float32 NOT NULL,"
            "journey_purpose_level String NOT NULL,"
            "journey_purpose String,"
            "pan_cnt UInt64 NOT NULL,"
            "journeys UInt32,"
            "txn_gbp_amt Float32 NOT NULL,"
            "txn_cnt UInt64 NOT NULL,"
            "day_of_week String NOT NULL,"
            'month Nullable(Int8),'
            'quarter Int8,'
            'year Int16,'
            'merchant_postal_sector Nullable(String),'
            'merchant_postal_district Nullable(String),'
            'merchant_postal_area Nullable(String),'
            'merchant_postal_town Nullable(String),'
            'merchant_postal_region Nullable(String),'
            'merchant_population Nullable(UInt32),'
            'merchant_centre_lat Nullable(Float32),'
            'merchant_centre_lng Nullable(Float32),'
            ") ENGINE = MergeTree() "
            "ORDER BY (time_frame, merchant_location, journey_purpose, day_of_week)"
)


passwords_dict['module3_sample_10k.zip']= None
passwords_dict['nr_module_3_2020.csv.zip']= b'NetworkRail_2020'
passwords_dict['nr_module_3_2021.csv.zip']= b'NetworkRail_2021'
passwords_dict['nr_module_3_2022.csv.zip']= b'NetworkRail_2022'
    
zip_filenames_dict['module3_sample_10k.zip']= 'module3_sample_10k.csv'
zip_filenames_dict['nr_module_3_2020.csv.zip']= 'san-ssapfs/edge/home/chaudhup/Network_Rail/nr_module_3_2020.csv'
zip_filenames_dict['nr_module_3_2021.csv.zip']= 'san-ssapfs/edge/home/chaudhup/Network_Rail/nr_module_3_2021.csv'
zip_filenames_dict['nr_module_3_2022.csv.zip']= 'san-ssapfs/edge/home/chaudhup/Network_Rail/nr_module_3_2022.csv'

def etl_sample_file():
    return etl(['data/module3_sample_10k.zip'], table_name='module3_sample')

def etl_real_files():
    return etl([
        '/mnt/sftp/in/nr_module_3_2021.csv.zip',
        # '/mnt/sftp/in/nr_module_3_2020.csv.zip',
        # '/mnt/sftp/in/nr_module_3_2022.csv.zip',
        ], table_name = 'module3')

def etl(infpaths, table_name = 'module3'):
    import time
    time0= time.perf_counter()
    census_dict = load_census_dict()
    print(f'Loading census and distance dict took {time.perf_counter() - time0}s.')

    dbinfo = DBInfo(db_creation_string_columns, table_name = table_name)
    apply_and_save(infpaths, convert_row, dbinfo, census_dict)

if __name__ == '__main__':
    import time
    time0 = time.perf_counter()

    etl_sample_file()

    print(f'Took {time.perf_counter() - time0} seconds to process {line_num} lines.')