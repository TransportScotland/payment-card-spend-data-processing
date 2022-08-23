import re
#TODO do this better, without a global line number variable
from shared_funcs import line_num

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

