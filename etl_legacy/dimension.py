import abc #abstract classes


class Dimension(metaclass=abc.ABCMeta):

    # constructor
    def __init__(self) -> None:
        self.headers = ()
        self.dim_list = []
        self.indices_dict = {}


    # returns ID of key just added or already existing
    def add_if_not_in(self, key, values : tuple):
        """
        Add an item to the dimension table in memory. Need to save later using to_sql once all filled in.
        Does not check if new values are the same as old values, only compares keys. 
        
        Parameters: 
            key: the unique identifier of the dimension entry
            values: tuple of values in the row of the table

        Returns:
            index (ID) of the key no matter if it was just inserted or already existing
        """
        if key in self.indices_dict:
            return self.indices_dict[key]
        else:
            idx = len(self.dim_list)
            self.dim_list.append(values)
            self.indices_dict[key] = idx
            return idx

    def to_sql(self, table_name, con):
        """Save dimension to MySQL db"""

        import table_info
        if table_name not in table_info.ALLOWED_TABLES:
            raise 'Table name not allowed: ' + table_name

        # add ID column at the end, where it is set up to be in table_info.create_strings (as a primary key)
        values = [value + (idx,) for idx, value in enumerate(self.dim_list)]

        headers = table_info.headers_dict[table_name]
        headers_str = '('+ ','.join(headers) + ')'
        values_f_str = '(' + ','.join(['%s' for _ in range(len(headers))]) + ')'

        sql = f"INSERT INTO {table_name} {headers_str} VALUES {values_f_str}"
        
        import shared_funcs
        shared_funcs.create_db_table(table_name, con)
        # executemany can be very fussy so this may be a good place to print things before sending if things go wrong 
        # print(sql)
        # print(values[:10])
        con.cursor().executemany(sql, values)

        # if unique_index_column in self.headers:
        #     con.cursor().execute(f'CREATE UNIQUE INDEX {unique_index_column} ON {table_name} ({unique_index_column})')
        con.commit()

    @classmethod
    def _from_dict(cls, dic, headers):
        """
        Creates a dimension from a dict like
        {key: (row_item1, row_item2, ...)}
        """
        new = cls()
        new.headers = headers
        for key, row in dic.items():
            id = new.add_if_not_in(key, row)
        return new
        # # possible alt option:
        # items = dic.items()
        # new.dim_list = [v for k, v in items]
        # new.indices_dict = {k: i for i, (k, v) in enumerate(items)}

    def get_index(self, key):
        return self.indices_dict[key]

    # override functions to make this class's interface work a lot like a dictionary
    def __contains__(self, key):
        return key in self.indices_dict

    def __getitem__(self, key):
        return self.dim_list[self.indices_dict[key]]

    def __setitem__(self, key, values: tuple):
        return self.add_if_not_in(key, values)

    def __str__(self):
        return f'{self.headers},\n{self.dim_list},\n{self.indices_dict}'

    def keys(self):
        return self.indices_dict.keys()

    def values(self):
        return self.dim_list

    def items(self):
        return [(k[0], self.dim_list[k[1]]) for k in self.indices_dict.items()]



class SimpleDimension(Dimension):
    """
    A simple dimension implementation for columns where the values are one element which is the same as the unique key.
    """

    def __init__(self, headers = ('value', )) -> None:
        super().__init__()
        self.headers = headers

    def add_row(self, row, col_index):
        """Add value from row at col_index to dimension table, get id"""
        val = row[col_index]
        id = self.add_if_not_in(val, (val,))
        return id

    
    @classmethod
    def from_dict_after_make_tuples(cls, dic, headers = ('value',)):
        """
        Creates a dimension from a dict like
        {key: value}
        """
        tupled_dict = {k: (k, v,) for k,v in dic.items()}
        return cls._from_dict(tupled_dict, headers)

class DistrictException(Exception):
    pass

class UnknownLocationError(Exception):
    #todo maybe have location as a contructor parameter but then this is python so it might be messy
    pass

class LocationDimension(Dimension):
    """
    A dimension for postcode sector locations
    """

    def __init__(self) -> None:
        super().__init__()
        import table_info
        #     'location': ('location', 'sector', 'district', 'area', 'region', 'location_level', 
        #         'latitude', 'longitude, 'population', 'area_ha', # 'id'),
        self.headers = table_info.headers_dict['location'][:-1]
    
    @staticmethod
    def postcode_sector_to_loc_list(sector: str):
        """
        Split up postcode sector into a tuple of (sector, district, area, region).
        Region is currently 'unknown', may be Scotland/England/NI/EU later
        """
        
        district = sector[:-2] # sector is always 2 chars longer than district (space and a digit)

        # get first character if second char is number, else first two chars.
        # todo confirm this is valid - some London postcode districts may cause issues because the include letters in the district part
        area = district[0] if district[1].isnumeric() else district[0:2]
        # re.match('^[A-Z]{1,2}', district) # slower but works in plain SQL (regex)

        # region might have to be done differently (current only known region is scotland, will need to classify by postcode area)
        region = 'unknown'

        return (sector, district, area, region)

    # TODO handle partially aggregated data
    def add_row(self, row, loc_level_idx = 4, loc_level = 5, smallest_area_name = 'POSTCODE_SECTOR', skip_bigger_area_rows = False):
        """
        Add an element from row at loc_idx to this dimension's table, if the value at loc_level_idx is smallest_area_name.
        Splits up postcode sectors to district and area parts too
        """
        loc = row[loc_level]
        loc_level = row[loc_level_idx]
        if (skip_bigger_area_rows):
            if loc_level != smallest_area_name: 
                # else:
                    # row is not of POSTCODE_SECTOR. maybe deal with this in a better way
                    # maybe instead just return a -1 because it will happen quite often and exceptions are slow
                raise DistrictException(f'Row at index {loc_level_idx} expected {smallest_area_name}, but got {row[loc_level_idx]}')

        if loc not in self.indices_dict:
            # unrecognised
            # temporarily splitting up only sectors
            if loc_level == smallest_area_name:
                sector, area, district, region = self.postcode_sector_to_loc_list(loc)
            else:
                sector, area, district, region = None, None, None, None
            id = self.add_if_not_in(loc,(loc, sector, area, district, region, loc_level, None, None, None, None))
        else:
            # print('recognised: '+ loc)
            id = self.indices_dict[loc]
        # maxid = len(self.dim_list)
        # id = self.add_if_not_in(loc, (loc, None, None, None, None, loc_level, None, None, None))
        # if id >= maxid:
        #     # newly added
        #     # print('Unrecognised location: ' + loc) # TODO save this to a file instead
        #     pass
        # return self.get_index(row[loc_level])
        return id


class TimeDimension(Dimension):
    """A dimension table for the time dimension"""

    def __init__(self) -> None:
        super().__init__()
        self.headers = ('time_raw', 'year', 'quarter', 'month') # todo consider getting this from table_info


    # convert time into year month and quarter and save to dimension
    def add_row(self, row, col_index = 0):
        """
        Add time at col_index to dimension table along with categorised by year, quarter, month
        """
        time_raw = str(row[col_index]) #optional depending on stuff
        if (time_raw[0].isnumeric()):
            # month data
            year = int(time_raw[:4]) #check if faster this or 202201 / 100 and 202201 % 100
            month = int(time_raw[4:6])
            quarter = int((month - 1) / 3) + 1
        else:
            # a quarter
            year = int('20' + time_raw[-2:])
            m1 = time_raw[:3]
            quarter = {'Jan': 1, 'Apr': 2, 'Jul': 3, 'Oct': 4}[m1]
            month = None

        # not sure if I actually need time_raw in the table but maybe there was a reason for it
        time_id = self.add_if_not_in(time_raw, (time_raw, year, quarter, month))
        return time_id


