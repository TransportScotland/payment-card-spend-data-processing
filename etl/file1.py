

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


line_num = 0
class InvalidDataError (Exception):
    def __init__(self, dimension, value, extra = ''):
        super().__init__(line_num, dimension, value, extra)
    pass

class MismatchedLocationLevel ():
    def __init__(self, return_anyway, expected_level, got_level, got_value):
        self.return_anyway = return_anyway
        self.expected_level = expected_level
        self.got_level = ('postcode_ '+ got_level).upper()
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
        raise InvalidDataError('location', location, 'Did not match any of sector, district, or area.')
    
    town, region = get_regions_file()[area]

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
distances_dict = {}

def prepare_distance_dict(fpath):
    import csv
    # global sqlitecon
    # con = sqlitecon
    # cur = con.cursor()
    # cur.execute("CREATE TABLE distance (origin varchar(6), \
    #     destination varchar(6), driving_seconds int, \
    #     primary key(origin, destination));")

    with open(fpath,'r') as file: # `with` statement available in 2.5+
        # csv.DictReader uses first line in file for column headings by default
        csv_reader = csv.reader(file)
        global distances_dict

        def line_to_dict(line):
            if (len(line) >=7 ):
                try:
                    return (line[0], line[1]), str(int(line[6]))
                except ValueError as ve:
                    return (line[0], line[1]), NULL
            elif (len(line) >= 2):
                return (line[0], line[1]), NULL
            else:
                return (NULL, NULL), NULL

        distances_dict = dict(line_to_dict(line) for line in csv_reader)
        # for chunk in read_distance_csv_chunk(csv_reader, 1000):
        # # dr = csv.DictReader(file) # comma is default delimiter
        # # to_db = [(i['col1'], i['col2']) for i in dr]

        #     cur.executemany("INSERT INTO distance (origin, destination, driving_seconds) VALUES (?, ?);", chunk)
        #     con.commit()
    # con.close()

def distance_col(origin_post, destination_post):
    try:
        return distances_dict[(origin_post, destination_post)]
    except KeyError:
        return NULL
    # c = sqlitecon.cursor()
    # c.execute("SELECT driving_seconds from distance where origin=? and destination=?", origin_post, destination_post)
    # return c.fetchone()
    pass


dbpassword = 'RadicalSpiderWearingPaper'

def getDbPassword():
    global dbpassword
    if dbpassword:
        return dbpassword
    else:
        dbpassword = input("Please enter the database password ")
        return dbpassword

def csv_to_db(fpath):
    import os

    database_name = 'tempdb'

    os.system(f'clickhouse-client --password={getDbPassword()}'
    f' --query="DROP TABLE IF EXISTS {database_name}.module1;"')

    os.system(f'clickhouse-client --password={getDbPassword()}'
        ' --query="'
        f"CREATE TABLE {database_name}.module1 ("
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
        'merchant_postal_sector Nullable(String),'
        'merchant_postal_district Nullable(String),'
        'merchant_postal_area Nullable(String),'
        'merchant_postal_town Nullable(String),'
        'merchant_postal_region Nullable(String),'
        'driving_seconds Nullable(UInt32)'
        ") ENGINE = MergeTree() "
        "ORDER BY (time_frame, cardholder_location, merchant_location)"
        ';"')

    os.system((f'clickhouse-client --password={getDbPassword()}'
    ' --format_csv_delimiter="|"'
    ' --input_format_csv_skip_first_lines=1'
    f' --query="INSERT INTO {database_name}.module1 FORMAT CSV" < {fpath}'))



def etl(infpath):


    outfpath = 'data/tmp_module1_t1.csv'
    errorfpath = 'error_file.txt'
    with open(infpath) as infile, open(outfpath, 'w') as outfile, open(errorfpath, 'w') as errorfile:
        prepare_distance_dict('generated_data/out_card_trips_car.csv')

        colsep = '|'
        headers = next(infile)
        outfile.write(headers.rstrip() + colsep.join([
            'month','quarter','year',
            'cardholder_postal_sector', 'cardholder_postal_district', 'cardholder_postal_area', 
            'cardholder_postal_town', 'cardholder_postal_region',
            'merchant_postal_sector', 'merchant_postal_district', 'merchant_postal_area', 
            'merchant_postal_town', 'merchant_postal_region',
            'driving_seconds'
         ]))
        for line in infile:
            global line_num
            line_num += 1
            try:
                linestrip = line.rstrip()
                split = linestrip.split(colsep)
                time_dim = time_cols(split[0])
                cardholder_location = location_cols(split[3], split[2])
                if isinstance(cardholder_location, MismatchedLocationLevel):
                    split[2] = cardholder_location.got_level
                    cardholder_location = cardholder_location.return_anyway
                merchant_location = location_cols(split[5], split[4])
                if isinstance(merchant_location, MismatchedLocationLevel):
                    split[4] = merchant_location.got_level
                    merchant_location = merchant_location.return_anyway
                distance = distance_col(split[3], split[5])
            except KeyboardInterrupt as ki:
                print(f'KeyboardInterrupt at infile line {line_num}')
                raise ki
            except Exception as e:
                errorfile.write(str(e))
                errorfile.write('\n')

            outfile.write(colsep.join([
                *split, 
                *time_dim, 
                *cardholder_location,
                *merchant_location,
                distance
            ])+ '\n') 


    csv_to_db(outfpath)
    


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