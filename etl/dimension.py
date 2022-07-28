import abc #abstract classes


class Dimension(metaclass=abc.ABCMeta):
    # headers = ()
    # dim_list = []
    # indices_dict = {}
    # python is SO annoying ughhgghgs

    def __init__(self) -> None:
        self.headers = ()
        self.dim_list = []
        self.indices_dict = {}

    def add_if_not_in(self, key, values : tuple):
        if key in self.indices_dict:
            return self.indices_dict[key]
        else:
            idx = len(self.dim_list)
            self.dim_list.append(values)
            self.indices_dict[key] = idx
            return idx

    # def add_index_to_list(lst):
    #     return [value + (idx,) for idx, value in enumerate(lst)]

    def to_sql(self, table_name, con, unique_index_column: None):
        import table_info
        if table_name not in table_info.ALLOWED_TABLES:
            raise 'Table name not allowed: ' + table_name


        # values = [tuple(row[:-1])for row in dicts[name].values()]
        # values = [tuple(row[:])for row in dicts[name].dim_list]
        # print(values)
        # headers = table_info.headers_dict[table_name]#[:-1]
        values = [value + (idx,) for idx, value in enumerate(self.dim_list)]
        # add_index_to_list(self.dim_list)

        headers = table_info.headers_dict[table_name]
        headers_str = '('+ ','.join(headers) + ')'
        values_f_str = '(' + ','.join(['%s' for _ in range(len(headers))]) + ')'

        sql = f"INSERT INTO {table_name} {headers_str} VALUES {values_f_str}"
        
        # print(table_name)
        # print(headers_str)
        # print(values[:10])
        # print(values)
        # print(sql)
        # print()
        # print(headers)
        # print(values_f_str)
        # print(dicts)
        import shared_funcs
        shared_funcs.create_db_table(table_name, con)
        con.cursor().executemany(sql, values)

        if unique_index_column in self.headers:
            con.cursor().execute(f'CREATE UNIQUE INDEX {unique_index_column} ON {table_name} ({unique_index_column})')
        con.commit()
        # raise NotImplementedError("Feature not implemented yet")

    def __contains__(self, key):
        return key in self.indices_dict

    def __getitem__(self, key):
        return self.dim_list[self.indices_dict[key]]

    def __setitem__(self, key, values: tuple):
        return self.add_if_not_in(key, values)
        # self.dim_list[self.indices_dict[key]] = value

    def __str__(self):
        return f'{self.headers},\n{self.dim_list},\n{self.indices_dict}'

    def keys(self):
        return self.indices_dict.keys()

    def values(self):
        return self.dim_list

    def items(self):
        return [(k[0], self.dim_list[k[1]]) for k in self.indices_dict.items()]



class SimpleDimension(Dimension):
    # headers = ('value')

    def __init__(self, headers = ('value', )) -> None:
        super().__init__()
        self.headers = headers

    def add(self, row, col_index):
        val = row[col_index]
        id = self.add_if_not_in(val, (val,))
        return id

class LocationDimension(Dimension):
    # headers = ('sector', 'district', 'area', 'region')
    def __init__(self) -> None:
        super().__init__()
        self.headers = ('sector', 'district', 'area', 'region')
    # split up postcode sector into a tuple of (sector, district, area, region)
    # region is currently 'unknown', may be Scotland/England/NI/EU later
    @staticmethod
    def postcode_sector_to_loc_list(sector: str):
        # district = sector.rsplit(' ', maxsplit=1)[0]
        district = sector[:-2]

        # first digit if second digit is number, else first two digits.
        # to confirm if any postcodes areas are 3 chars (simple loop then)
        area = district[0] if district[1].isnumeric() else district[0:2]

        # region might have to be done differently (current only known one is scotland)
        region = 'unknown'

        return (sector, district, area, region)

    def add(self, row, loc_level_idx = 4, loc_idx = 5, smallest_area_name = 'POSTCODE_SECTOR'):
        # there may be a better way to do this than a star schema
        if row[loc_level_idx] != smallest_area_name:
            # probably skip adding this line to the fact table.
            # but do save it somewhere to check the sub-sections add up to the right number
            # skip handling this until data is known
            return -1
        sector = row[loc_idx]
        loc_list = self.postcode_sector_to_loc_list(sector)

        id = self.add_if_not_in(sector, loc_list)
        return id

class TimeDimension(Dimension):
    # headers = ('time_raw', 'year', 'quarter', 'month')
    def __init__(self) -> None:
        super().__init__()
        self.headers = ('time_raw', 'year', 'quarter', 'month')
    def add(self, row, col_index = 0):
        time_raw = str(row[col_index]) #optional depending on stuff
        if (not time_raw[0].isnumeric()):
            # a quarter - skipping for now
            raise
        year = int(time_raw[:4]) #check if faster this or 202201 / 100 and 202201 % 100
        month = int(time_raw[4:6])
        quarter = int(month / 3)

        # not sure if I actually need time_raw in the table but maybe there was a reason for it
        time_id = self.add_if_not_in(time_raw, (time_raw, year, quarter, month))
        return time_id


