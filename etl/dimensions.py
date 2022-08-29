import re
#TODO do this better, without a global line number variable
from shared_funcs import line_num

# defining NULL_CELL as an empty string here so that it will be empty in the csv 
# but easy to change to NoneType if needed
NULL_CELL = "" 

regions_file = {}
def get_regions_file():
    """
    Get (or open and get) regions dict like:
    {Area: (Postal town, Region)}
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
            # add null values
            regions_file[None] = NULL_CELL
            regions_file[NULL_CELL] = NULL_CELL
            return regions_file


def get_region_info(key):
    """Get region info at key (area), get tuple (Postal town, region)"""
    try:
        return get_regions_file()[key]
    except KeyError as ke:
        return (NULL_CELL,) * 2

class InvalidDataError (Exception):
    def __init__(self, dimension, value, extra = ''):
        super().__init__(line_num, dimension, value, extra)
    pass

class MismatchedLocationLevel ():
    """
    Return this instead of raising an exception when only a warning is wanted
    Maybe a weird way of doing things. Want to give the caller an option to throw an exception, or ignore it and just get the result.
    """
    def __init__(self, return_anyway, expected_level, got_level, got_value):
        """
        return_anyway is the row as if nothing went wrong (with fixed value)
        """
        self.return_anyway = return_anyway
        self.expected_level = expected_level
        self.got_level = (f'postcode_ {got_level}').upper()
        self.got_value = got_value

        if print_warnings:
            msg = f'Mismatched location_level and true location: expected {expected_level}, but got {got_value} which matches {got_level}'
            InvalidDataWarning('location', got_value, msg)


print_warnings = False
def InvalidDataWarning (dimension, value, extra):
    """Print a warning if print_warnings is set to True"""
    if print_warnings:
        print(f'Warning on line {line_num} processing {dimension}, '
            f'got unexpected value {value}. Extra message provided: {extra}')
        


def time_cols(time: str):
    """
    Handle time_frame.
    returns (month, quarter, year)
    """
    if len(time) == 15:
        #quarter data (Jan 21 - Apr 21)
        year = '20' + time[4:6]
        month = NULL_CELL
        quarter = {
            'Jan': 1, 'Apr': 2, 'Jul': 3, 'Oct': 4
        }[time[0:3]]
        pass
    elif len(time) == 6 and time[4] == 'Q':
        # also quarter data but in the form 2021Q1 (module3)
        month = NULL_CELL
        quarter = time[5]
        year = time[0:4]
    elif len(time) == 6:
        # month data (202101)
        year = time[0:4]
        month = time[4:6]
        quarter = int((int(month) - 1) / 3) + 1
        pass
    else:
        # invalid time
        raise InvalidDataError('time', time, f'Unexpected length of time, expected 6 or 15 but got {len(time)}')
        pass
    return month, str(quarter), year
    pass

# pre-compile regex to speed up matching
re_sector = re.compile(r'\b[A-Z]{1,2}\d{1,2}[A-Z]? \d') #1 or 2 capital letters, 1 or 2 digits, optional capital letter, a space, a digit
re_district = re.compile(r'\b[A-Z]{1,2}\d{1,2}[A-Z]?') # as above minus space and digit
re_area = re.compile(r'\b[A-Z]{1,2}') # 1 or 2 capital letters

def location_cols(location: str, meta: str):
    """
    Handle location columns.
    Returns (sector, district, area, town_name, region), or MismatchedLocationLevel if meta did not match actual granularity of location.
    """
    # cannot split up overall
    if meta == 'OVERALL' or meta == 'Overall' or meta == 'POSTCODE_OVERALL' or meta =='Postcode_Overall':
        return (NULL_CELL,) * 5
    # no splitting up of countries or regions (e.g. North West England)
    if meta == 'POSTCODE_REGION' or meta == 'Postcode_Region' or \
        meta == 'COUNTRY' or meta == 'Country':
        # only convert to Title Case
        return (NULL_CELL,) * 4 + (location.title(),)
    
    # see what matches
    sector_m = re_sector.match(location)
    district_m = re_district.match(location)
    area_m = re_area.match(location)

    # get matched str or null
    sector = sector_m[0] if sector_m else NULL_CELL
    district = district_m[0] if district_m else NULL_CELL
    area = area_m[0] if area_m else NULL_CELL

    smallest_div = 'sector' if sector else 'district' if district else 'area' if area else None
    if smallest_div == None:
        # this should never happen - means the location_level was none of the above checks and location regex did not match
        # raise InvalidDataError('location', location, 'Did not match any of sector, district, or area.')
        return_anyway = (NULL_CELL,) * 5
        return MismatchedLocationLevel(return_anyway, meta, smallest_div, location)
        InvalidDataWarning('location', location,
            f'Mismatched location_level and true location: expected {meta}, but got {location} which matches {smallest_div}')


    town, region = get_region_info(area)

    if ('postcode_' + smallest_div).upper() != meta.upper():
        # area BH is sometimes classed as POSTCODE_DISTRICT in input data
        return_anyway = sector, district, area, town, region
        return MismatchedLocationLevel(return_anyway, meta, smallest_div, location)
        InvalidDataWarning('location', location,
            f'Mismatched location_level and true location: expected {meta}, but got {location} which matches {smallest_div}')


    return sector, district, area, town, region
    pass



def load_distance_dict(fpath = 'generated_data/distances_dict.json'):
    """Load distnaces json (see distances_to_json.py)"""
    import json
    # global distances_dict
    with open(fpath) as injson:
        distances_dict = json.loads(injson.read())
        return distances_dict

def _distance_dict_key(origin, dest):
    """Get a key for the distances dict (json does not support tuple keys so using str)"""
    return ','.join((origin, dest))

def distance_col(distances_dict, origin_post, destination_post):
    """
    Get distance cell from distances_dict at origin_post, destination_post. Or return null cell if not found.
    Distances dict must be loaded first
    """
    try:
        return str(distances_dict[_distance_dict_key(origin_post, destination_post)])
    except KeyError:
        return NULL_CELL
    pass

def load_census_dict(fpath = 'generated_data/census_dict.json'):
    """Load in census dict (with locations too) from json (see census_to_json.py)"""
    #TODO currently this is being called from each file module but it is used in all of them so could be moved to shared_funcs
    import json
    with open(fpath) as injson:
        census_dict = json.loads(injson.read())
        return census_dict

def census_info_col(census_dict, location):
    """
    Get a column cell of census info. Census dict must be loaded first.
    Returns (population, centre_latitude, centre_longitude) or null if not found
    """
    try:
        row = census_dict[location]
        # return row.items()
        return tuple(str(e) for e in (row['population'], row['lat'], row['lng']))
    except KeyError:
        return (NULL_CELL,) * 3

