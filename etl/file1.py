
from dimensions import *
from shared_funcs import *

def convert_row(split, dist_dict, census_dict) -> list:
    # outfheaders = (headers.rstrip() + colsep.join([
    #     'month','quarter','year',
    #     'cardholder_postal_sector', 'cardholder_postal_district', 'cardholder_postal_area', 
    #     'cardholder_postal_town', 'cardholder_postal_region',
    #     'merchant_postal_sector', 'merchant_postal_district', 'merchant_postal_area', 
    #     'merchant_postal_town', 'merchant_postal_region',
    #     'driving_seconds'
    #  ]) + '\n')

    time_dim = time_cols(split[0])
    cardholder_location = location_cols(split[3], split[2])
    if isinstance(cardholder_location, MismatchedLocationLevel):
        split[2] = cardholder_location.got_level
        cardholder_location = cardholder_location.return_anyway
    merchant_location = location_cols(split[5], split[4])
    if isinstance(merchant_location, MismatchedLocationLevel):
        split[4] = merchant_location.got_level
        merchant_location = merchant_location.return_anyway
    distance = distance_col(dist_dict, split[3], split[5])
    cardholder_census_info = census_info_col(census_dict, split[3])
    merchant_census_info = census_info_col(census_dict, split[5])
    return [
        *split, 
        *time_dim, 
        *cardholder_location,
        *cardholder_census_info,
        *merchant_location,
        *merchant_census_info,
        distance
    ]
    
db_creation_string_columns = str(
            "time_frame String NOT NULL,"
            "cardholder_type String NOT NULL,"
            "cardholder_location_level String NOT NULL,"
            "cardholder_location String NOT NULL,"
            "merchant_location_level String NOT NULL,"
            "merchant_location String NOT NULL,"
            "pan_cnt UInt32 NOT NULL,"
            "txn_cnt UInt64 NOT NULL,"
            "txn_gbp_amt Float32 NOT NULL,"
            "mcc_rank1 String NOT NULL,"
            "mcc_rank2 String NOT NULL,"
            "mcc_rank3 String NOT NULL,"
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
            'cardholder_centre_lng Nullable(Float32),'
            'merchant_postal_sector Nullable(String),'
            'merchant_postal_district Nullable(String),'
            'merchant_postal_area Nullable(String),'
            'merchant_postal_town Nullable(String),'
            'merchant_postal_region Nullable(String),'
            'merchant_population Nullable(UInt32),'
            'merchant_centre_lat Nullable(Float32),'
            'merchant_centre_lng Nullable(Float32),'
            'distance_driving_seconds Nullable(UInt32)'
            ") ENGINE = MergeTree() "
            "ORDER BY (time_frame, cardholder_location, merchant_location)"
)


passwords_dict['module1_sample_10k.zip']= None
passwords_dict['nr_module_1_2020.csv.zip']= b'NetworkRail_2020'
passwords_dict['nr_module_1_2021.csv.zip']= b'NetworkRail_2021'
passwords_dict['nr_module_1_2022.csv.zip']= b'NetworkRail_2022'
    
zip_filenames_dict['module1_sample_10k.zip']= 'module1_sample_10k.csv'
zip_filenames_dict['nr_module_1_2020.csv.zip']= 'san-ssapfs/edge/home/chaudhup/Network_Rail/nr_module_1_2020.csv'
zip_filenames_dict['nr_module_1_2021.csv.zip']= 'san-ssapfs/edge/home/chaudhup/Network_Rail/nr_module_1_2021.csv'
zip_filenames_dict['nr_module_1_2022.csv.zip']= 'san-ssapfs/edge/home/chaudhup/Network_Rail/nr_module_1_2022.csv'

def etl_sample_file():
    return etl(['data/module1_sample_10k.zip'], table_name='module1_sample')

def etl_real_files():
    return etl([
        '/mnt/sftp/module 1/nr_module_1_2021.csv.zip',
        '/mnt/sftp/in/nr_module_1_2020.csv.zip',
        '/mnt/sftp/in/nr_module_1_2022.csv.zip',
        ], table_name = 'module1')

def etl(infpaths, table_name = 'module1'):
    import time
    time0= time.perf_counter()
    # census_dict, dist_dict = {}, {}
    census_dict = load_census_dict()
    dist_dict = load_distance_dict()
    print(f'Loading census and distance dict took {time.perf_counter() - time0}s.')

    dbinfo = DBInfo(db_creation_string_columns, table_name = table_name)
    apply_and_save(infpaths, convert_row, dbinfo, census_dict, dist_dict)

if __name__ == '__main__':
    import time
    time0 = time.perf_counter()

    etl_sample_file()
    # etl('/mnt/sftp/module 1/san-ssapfs/edge/home/chaudhup/Network_Rail/nr_module_1_2021.csv')

    print(f'Took {time.perf_counter() - time0} seconds to process {line_num} lines.')