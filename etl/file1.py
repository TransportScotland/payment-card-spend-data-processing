

import re


# defining NULL as an empty string here so that it will be empty in the csv 
# but easy to change to NoneType if needed
NULL = "" 

regions_file = {}
def get_regions_file():
    """
    Area: (Postal town, Region)
    """
    global regions_file
    if regions_file:
        return regions_file
    else:
        with open('other_data/postcode_regions.tsv') as file:
            def line_to_region_tuple(line):
                s = line.rstrip().split('\t')
                return s[0], (s[1], s[2])
            regions_file = dict(line_to_region_tuple(line) for line in file)
            regions_file[None] = NULL
            regions_file[NULL] = NULL
            return regions_file


def get_region_info(key):
    try:
        return get_regions_file()[key]
    except KeyError as ke:
        return (NULL,) * 2

line_num = 0
class InvalidDataError (Exception):
    def __init__(self, dimension, value, extra = ''):
        super().__init__(line_num, dimension, value, extra)
    pass

class MismatchedLocationLevel ():
    def __init__(self, return_anyway, expected_level, got_level, got_value):
        self.return_anyway = return_anyway
        self.expected_level = expected_level
        self.got_level = (f'postcode_ {got_level}').upper()
        self.got_value = got_value

        if print_warnings:
            msg = f'Mismatched location_level and true location: expected {expected_level}, but got {got_value} which matches {got_level}'
            InvalidDataWarning('location', got_value, msg)


print_warnings = False
def InvalidDataWarning (dimension, value, extra):
    if print_warnings:
        print(f'Warning on line {line_num} processing {dimension}, '
            f'got unexpected value {value}. Extra message provided: {extra}')
        


def time_cols(time: str):
    if len(time) == 6 :
        # month data
        year = time[0:4]
        month = time[4:6].replace('0','')
        quarter = int((int(month) - 1) / 3) + 1
        pass
    elif len(time) == 15:
        #quarter data
        year = '20' + time[4:6]
        month = NULL
        quarter = {
            'Jan': 1, 'Apr': 2, 'Jul': 3, 'Oct': 4
        }[time[0:3]]
        pass
    else:
        # invalid time
        raise InvalidDataError('time', time, f'Unexpected length of time, expected 6 or 15 but got {len(time)}')
        pass
    return month, str(quarter), year
    pass

re_sector = re.compile(r'\b[A-Z]{1,2}\d{1,2}[A-Z]? \d')
re_district = re.compile(r'\b[A-Z]{1,2}\d{1,2}[A-Z]?')
re_area = re.compile(r'\b[A-Z]{1,2}')

def location_cols(location: str, meta: str):
    if meta == 'OVERALL' or meta == 'Overall' or meta == 'POSTCODE_OVERALL' or meta =='Postcode_Overall':
        return (NULL,) * 5
    if meta == 'POSTCODE_REGION' or meta == 'Postcode_Region' or \
        meta == 'COUNTRY' or meta == 'Country':
        return (NULL,) * 4 + (location.title(),)
    sector_m = re_sector.match(location)
    district_m = re_district.match(location)
    area_m = re_area.match(location)
    # todo handle regions

    # maybe handle miscategorisations in data (area BH is classed as a district sometimes)
    sector = sector_m[0] if sector_m else NULL
    district = district_m[0] if district_m else NULL
    area = area_m[0] if area_m else NULL

    smallest_div = 'sector' if sector else 'district' if district else 'area' if area else None
    if smallest_div == None:
        # raise InvalidDataError('location', location, 'Did not match any of sector, district, or area.')
        return_anyway = (NULL,) * 5
        return MismatchedLocationLevel(return_anyway, meta, smallest_div, location)
        InvalidDataWarning('location', location,
            f'Mismatched location_level and true location: expected {meta}, but got {location} which matches {smallest_div}')


    town, region = get_region_info(area)

    if ('postcode_' + smallest_div).upper() != meta.upper():
        return_anyway = sector, district, area, town, region
        return MismatchedLocationLevel(return_anyway, meta, smallest_div, location)
        InvalidDataWarning('location', location,
            f'Mismatched location_level and true location: expected {meta}, but got {location} which matches {smallest_div}')


    return sector, district, area, town, region
    pass

def read_distance_csv_chunk(csv_reader, chunksize):
    for line in csv_reader:
        chunk = []
        chunk.append(tuple(line[0], line[1], line[6]))
        if len(chunk == chunksize):
            yield chunk
            chunk = []

import sqlite3
# sqlitecon = sqlite3.connect("sqlite://home/rdp/code/payment-card-data-processing/other_data/distance_sqlite.db")

# todo pickle this to make loading faster
# distances_dict = {}

def load_distance_dict(fpath = 'generated_data/distances_dict.json'):
    import json
    # global distances_dict
    with open(fpath) as injson:
        distances_dict = json.loads(injson.read())
        return distances_dict

def _distance_dict_key(origin, dest):
    return ','.join((origin, dest))

def distance_col(distances_dict, origin_post, destination_post):
    try:
        return str(distances_dict[_distance_dict_key(origin_post, destination_post)])
    except KeyError:
        return NULL
    # c = sqlitecon.cursor()
    # c.execute("SELECT driving_seconds from distance where origin=? and destination=?", origin_post, destination_post)
    # return c.fetchone()
    pass

def load_census_dict(fpath = 'generated_data/census_dict.json'):
    import json
    with open(fpath) as injson:
        census_dict = json.loads(injson.read())
        return census_dict

def census_info_col(census_dict, location):
    try:
        row = census_dict[location]
        # return row.items()
        return tuple(str(e) for e in (row['population'], row['lat'], row['lng']))
    except KeyError:
        return (NULL,) * 3


dbpassword = 'RadicalSpiderWearingPaper'

def getDbPassword():
    global dbpassword
    if dbpassword:
        return dbpassword
    else:
        dbpassword = input("Please enter the database password ")
        return dbpassword

def csv_to_db_create(dbname, table_name = 'module1'):
    import os


    os.system(f'clickhouse-client --password={getDbPassword()}'
    f' --query="DROP TABLE IF EXISTS {dbname}.{table_name};"')

    os.system(f'clickhouse-client --password={getDbPassword()}'
        ' --query="'
        f"CREATE TABLE {dbname}.{table_name} ("
        "time_frame String NOT NULL,"
        "cardholder_type String NOT NULL,"
        "cardholder_location_level String NOT NULL,"
        "cardholder_location String NOT NULL,"
        "merchant_location_level String NOT NULL,"
        "merchant_location String NOT NULL,"
        "pan_cnt UInt32 NOT NULL,"
        "txn_cnt UInt32 NOT NULL,"
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
        ';"')


#TODO load from a file
def get_zip_pwd(infpath):
    import os
    return {
        'module1_sample_10k.zip': None,
        'nr_module_1_2020.csv.zip': b'NetworkRail_2020',
        'nr_module_1_2021.csv.zip': b'NetworkRail_2021',
        'nr_module_1_2022.csv.zip': b'NetworkRail_2022',
    }[os.path.basename(infpath)]

def get_filename_in_zip(infpath):
    import os
    return {
        'module1_sample_10k.zip': 'module1_sample_10k.csv',
        'nr_module_1_2020.csv.zip': 'san-ssapfs/edge/home/chaudhup/Network_Rail/nr_module_1_2020.csv',
        'nr_module_1_2021.csv.zip': 'san-ssapfs/edge/home/chaudhup/Network_Rail/nr_module_1_2021.csv',
        'nr_module_1_2022.csv.zip': 'san-ssapfs/edge/home/chaudhup/Network_Rail/nr_module_1_2022.csv',
    }[os.path.basename(infpath)]

def split_row_generator(infpaths, outfpath, errorfpath, colsep):
    for infpath in infpaths:
        import zipfile
        with zipfile.ZipFile(infpath) as zf:
            # with open(outfpath, 'w') as outfile, open(errorfpath, 'w') as errorfile:
                with zf.open(get_filename_in_zip(infpath), pwd = get_zip_pwd(infpath)) as infile:
                    # colsep = '|'
                    csv_batch_size = 100000
                    headers = next(infile)
                    print(headers)
                    # outfheaders = (headers.rstrip() + colsep.join([
                    #     'month','quarter','year',
                    #     'cardholder_postal_sector', 'cardholder_postal_district', 'cardholder_postal_area', 
                    #     'cardholder_postal_town', 'cardholder_postal_region',
                    #     'merchant_postal_sector', 'merchant_postal_district', 'merchant_postal_area', 
                    #     'merchant_postal_town', 'merchant_postal_region',
                    #     'driving_seconds'
                    #  ]) + '\n')
                    # outfile.write(outfheaders)

                    for line in infile:
                        global line_num
                        line_num += 1

                        if isinstance(line, bytes):
                            line = line.decode('utf-8')
                        linestrip = line.rstrip()
                        split = linestrip.split(colsep)
                        yield split


class CsvDatabaseWriter:
    def __init__(self, outfpath, batch_size, dbname, table_name) -> None:
        self.dbname = dbname
        self.batch_size = batch_size
        self.outfpath = outfpath
        self.table_name = table_name
        self.line_counter = 0

    def open(self, ):
        # self.outfpath = outfpath
        self.outfile = open(self.outfpath, 'w')
        return self

    def close(self):
        # first check if any lines left in current batch
        if self.line_counter % self.batch_size != 0:
            self._save_to_db(False)
        return self.outfile.close()

    def _save_to_db(self, re_open_writing = True):
        self.outfile.close()

        # TODO fire this off in a separate thread, and either wait for prev to finish every time 
        # or have one csv file for each thread (which needs to be deleted) 
        import subprocess
        subprocess.Popen((f'clickhouse-client --password={getDbPassword()}'
        ' --format_csv_delimiter="|"'
        ' --input_format_csv_skip_first_lines=0'
        f' --query="INSERT INTO {self.dbname}.{self.table_name} FORMAT CSV" < "{self.outfpath}"'),
        shell=True).wait()

        if re_open_writing:
            self.outfile = open(self.outfpath, 'w')


    def write(self, line, colsep):
        return_value = self.outfile.write(colsep.join(line)+ '\n')
        # save to csv if batch filled
        self.line_counter += 1
        if self.line_counter % self.batch_size == 0:
            self._save_to_db()
        return return_value



def etl(infpaths):
    census_dict = load_census_dict()
    dist_dict = load_distance_dict()

    # todo look into alternative file types for the intermediate file, csv too big (zipped may be fine)
    # outfpath = '/mnt/sftp/module 1/tmp_module1_t1.csv'
    outfpath = 'data/tmp_module1_t1.csv'
    errorfpath = 'error_file.txt'
    error_count = 0
    max_errors_before_crash = 100000
    dbname = 'nr'
    csv_to_db_create(dbname)
    colsep = '|'


    with CsvDatabaseWriter(outfpath, 100000, 'nr', 'module1').open() as writer:
        pass
        for split in split_row_generator(infpaths, outfpath, errorfpath):
    # TODO turn this mess into a generator function
    # for infpath in infpaths:
    #     import zipfile
    #     with zipfile.ZipFile(infpath) as zf:
    #         with open(outfpath, 'w') as outfile, open(errorfpath, 'w') as errorfile:
    #             with zf.open(get_filename_in_zip(infpath), pwd = get_zip_pwd(infpath)) as infile:
    #                 colsep = '|'
    #                 csv_batch_size = 100000
    #                 headers = next(infile)
    #                 print(headers)
    #                 # outfheaders = (headers.rstrip() + colsep.join([
    #                 #     'month','quarter','year',
    #                 #     'cardholder_postal_sector', 'cardholder_postal_district', 'cardholder_postal_area', 
    #                 #     'cardholder_postal_town', 'cardholder_postal_region',
    #                 #     'merchant_postal_sector', 'merchant_postal_district', 'merchant_postal_area', 
    #                 #     'merchant_postal_town', 'merchant_postal_region',
    #                 #     'driving_seconds'
    #                 #  ]) + '\n')
    #                 # outfile.write(outfheaders)

    #                 for line in infile:
    #                     global line_num
    #                     line_num += 1
    #                     try:
    #                         if isinstance(line, bytes):
    #                             line = line.decode('utf-8')
    #                         linestrip = line.rstrip()
    #                         split = linestrip.split(colsep)



    #                 # outfheaders = (headers.rstrip() + colsep.join([
    #                 #     'month','quarter','year',
    #                 #     'cardholder_postal_sector', 'cardholder_postal_district', 'cardholder_postal_area', 
    #                 #     'cardholder_postal_town', 'cardholder_postal_region',
    #                 #     'merchant_postal_sector', 'merchant_postal_district', 'merchant_postal_area', 
    #                 #     'merchant_postal_town', 'merchant_postal_region',
    #                 #     'driving_seconds'
    #                 #  ]) + '\n')
                        try:
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

                            writer.write([
                                *split, 
                                *time_dim, 
                                *cardholder_location,
                                *cardholder_census_info,
                                *merchant_location,
                                *merchant_census_info,
                                distance
                            ], colsep)
                            # outfile.write(colsep.join([
                            #     *split, 
                            #     *time_dim, 
                            #     *cardholder_location,
                            #     *cardholder_census_info,
                            #     *merchant_location,
                            #     *merchant_census_info,
                            #     distance
                            # ])+ '\n')
                        except KeyboardInterrupt as ki:
                            print(f'KeyboardInterrupt at infile line {line_num}')
                            raise ki
                        except Exception as e:
                            print(line)
                            import traceback
                            tb = traceback.format_exc()
                            print(tb)
                            errorfile.write(line)
                            errorfile.write('#')
                            errorfile.write(str(tb))
                            errorfile.write('\n')
                            error_count += 1
                            if error_count > max_errors_before_crash:
                                raise 


            

                        # if line_num % csv_batch_size == 0:
                        #     outfile.close()
                        #     csv_to_db_add(outfpath, dbname)
                        #     outfile = open(outfpath, 'w')
                # if line_num % csv_batch_size != 0:
                #     outfile.close()
                #     csv_to_db_add(outfpath, dbname)
                #     outfile = open(outfpath,'w')
                
        print(f'Loaded {writer.line_counter} lines into the database.')

def etl_sample_file():
    return etl(['data/module1_sample_10k.zip'])

def etl_real_files():
    return etl([
        '/mnt/sftp/module 1/nr_module_1_2021.csv.zip',
        '/mnt/sftp/in/nr_module_1_2020.csv.zip',
        '/mnt/sftp/in/nr_module_1_2022.csv.zip',
        ])

if __name__ == '__main__':
    import time
    time0 = time.perf_counter()

    # import sys
    # if (len(sys.argv) == 3 and sys.argv[1] == 'timetest' and sys.argv[2].isnumeric()):
    #     print_warnings = False
    #     for i in range(int(sys.argv[2])):

    #         etl('data/module1_sample.csv')

    # else:
    #     etl('data/module1_sample_10k.csv')
    # etl('data/module1_sample_10k.csv')
    etl('/mnt/sftp/module 1/san-ssapfs/edge/home/chaudhup/Network_Rail/nr_module_1_2021.csv')

    print(f'Took {time.perf_counter() - time0} seconds to process {line_num} lines.')