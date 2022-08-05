import csv
import shared_funcs


def each_row(row, population_dict):
    """
    Adds data from row[1] to population_dict with key from row[0], collecting any split up values together.
    
    Parameters:
        row: tuple or list with postcode sector at element 0 and a number at element 1
        population_dict: Dictionary which will be modified to add the new row of data
    """
    loc_raw = row[0]
    population = int(row[1].replace(',', ''))

    #turn "AB12 3 (part) Aberdeen City" to just "AB12 3" (do nothing if shorter)
    loc = loc_raw if len(loc_raw) <=6 else ' '.join(loc_raw.split(' ', 2)[:2])

    # if already in
    if loc in population_dict:
        population_dict[loc] += population # add to existing
    else:
        population_dict[loc] = population # create new dict element with this value


def add_up_sectors(dic):
    """
    Add up the values in each sector to a district and area, modifying the input dictionary to add the new values

    Parameters:
        dic: dictionary of {postcode_sector: value_to_add_up}. Will be modified.
    """

    # todo consider instead returning a new dict rather than modifying current one.
    sectors = set(dic.keys())
    for sector in sectors:
        _, district, area, _ = shared_funcs.postcode_sector_to_loc_list(sector)
        
        if district not in dic:
            dic[district] = 0
        dic[district] += dic[sector]

        if area not in dic:
            dic[area] = 0
        dic[area] += dic[sector]


def change_dict_structure(dic):
    """Changes dict from {location: population} to {location: (location, population, random_index)}"""
    for i, (loc, population) in enumerate(dic.items()):
        dic[loc] = (loc, population, i)

def get_filled_population_dict(fpath):
    """
    Reads the file at fpath and fills a dictionary with the values in it useing census_data.each_row. 
    The file is expected to be in the format that the Scottish census data is in, 
    with 6 non-useful rows before the actual data and the end of data specified by an empty line (or end of file)
    """
    population_dict = {}
    with open (fpath) as file:
        # skip first 6 rows - meta info plus a row of total scotland data
        for _ in range(6):
            _ = next(file)

        # for each row in the remainder of the CSV
        csvreader = csv.reader(file)
        for row in csvreader:
            if not row[0]:
                # end loop if first element empty (the csv contains a few empty rows after the data before some meta information)
                break
            # add the population value of the current row to the dictionary
            each_row(row, population_dict)
    return population_dict


def save_to_sql(population_dict):
    """
    Saves the population_dict to a sql database defined in shared_funcs.get_sqlalchemy_con.

    Parameters:
        population_dict: dict of {location: population}
    Returns:
        A pandas DataFrame similar to the one saved to the database, for easier printing.
    """
    import table_info
    table_headers= table_info.headers_dict['census'] # location, population, id

    import dimension
    dim = dimension.SimpleDimension.from_dict_after_make_tuples(population_dict, table_headers[:2])
    shared_funcs.dicts['census'] = dim
    shared_funcs.save_dim('census')


    # returning a df for backward comp

    # pandas is slow but the census data currently isn't big enough to matter
    import pandas as pd

    # convert dict to df with rows like (id, location, population), taking the column names from table_info headers
    df = pd.DataFrame(data=zip(population_dict.keys(), population_dict.values()), columns=table_headers[:2])

    # save to sql, renaming the index to the index column specified in table_info headers
    # df.to_sql('census', shared_funcs.get_sqlalchemy_con(), index_label=table_headers[-1], if_exists='replace')
    return df

def etl(postcode_fpath, census_fpath):
    # might actually be better using pandas here than my own solution because I'm merging two tables
    import pandas as pd
    
    dfp = pd.read_csv(postcode_fpath, header=None)
    dfp = dfp[[0,1,2,6,7,8,9,12,13,14]]
    dfp = dfp.rename(columns={0: 'postcode', 1: 'status', 2: 'usertype', 6:'country', 7:'lat', 8:'lng', 
        9:'postcode_no_space', 12: 'area', 13:'district', 14:'sector'})
    print(dfp)

    dfp = dfp[dfp['status'] == 'live']
    dfp = dfp[dfp['usertype'] == 'small']
    print(dfp)


    avg_locs = dfp.groupby('sector')[['lat', 'lng']].mean()
    print(avg_locs)
    # avg_lats = dfp.groupby('sector')['lat'].mean()
    # avg_lngs = dfp.groupby('sector')['lng'].mean()
    # print(avg_lats)
    # print(avg_lngs)

    dfp = dfp.drop_duplicates(subset='sector', keep='first')
    dfp = dfp[['status', 'country', 'area', 'district', 'sector']]
    dfp = dfp.merge(avg_locs, left_on='sector', right_index=True)
    # dfp = dfp.merge(avg_lats, left_on=14, right_index=True)
    # dfp = dfp.merge(avg_lngs, left_on=14, right_index=True)


    print(dfp)
    

    dfc = pd.read_csv(census_fpath, header=None, skiprows=6, thousands=',')
    dfc = dfc[:-4]
    dfc = dfc[[0,1,2,3,7]]
    dfc = dfc.rename(columns={0: 'sector', 1:'population', 2:'pop_male', 3: 'pop_female', 7: 'area_size'})
    dfc = dfc.convert_dtypes() # make it stop converting ints to object by converting 'int' to 'Int64'
    # dfc['population'] = dfc['population'].astype(int)
    # dfc['pop_male'] = dfc['pop_male'].astype(int)
    # dfc['pop_female'] = dfc['pop_female'].astype(int)
    # dfc['area_size'] = dfc['area_size'].astype(int)

    # dfc_mix = dfc[dfc['sector'].str.len() > 7]
    # print(dfc_mix)
    dfc['sector'] = dfc['sector'].str.extract(r'(\b\w\w?\d[\d\w]? \d)')
    print(dfc)
    print(dfc.dtypes)
    dfc = dfc.groupby('sector')[['population', 'pop_male', 'pop_female', 'area_size']].sum()
    print(dfc)
    dfc = dfc.reset_index()
    # print(dfc.dtypes)
    # dfc = dfc.groupby('sector').sum()
    # dfc = dfc.rename(columns = {'Unnamed: 0': 'location'})
    print(dfc)

    #calculating density manually because some postcode sectors are split into  parts
    
    dfm = pd.merge(dfp, dfc, left_on='sector', right_on='sector', how='outer')
    # dfm = dfm.fillna(0)
    print(dfm)

    dfm['location_level'] = 'POSTCODE_SECTOR'
    dfm['location'] = dfm['sector']
    
    def drill_up(df: pd.DataFrame, col_name, location_level, 
            cols_sum = ['population', 'pop_male', 'pop_female', 'area_size'],
            cols_mean = ['lat', 'lng'], 
            cols_first = None):
        dfg = df.groupby(col_name)
        dfs=dfm=dff=None
        if cols_sum:
            dfs = dfg[cols_sum].sum()
        if cols_mean:
            dfm = dfg[cols_mean].mean()
        if cols_first:
            dff = dfg[cols_first].first()
        dfo = pd.concat([dfs, dfm, dff], axis=1)
        dfo = dfo.reset_index()
        dfo['location_level'] = location_level
        dfo['location'] = dfo[col_name]
        # print(dfo)
        return dfo

    dfmd = drill_up(dfm, 'district', 'POSTCODE_DISTRICT', cols_first=['area', 'country'])
    dfma = drill_up(dfm, 'area', 'POSTCODE_AREA', cols_first=['country'])
    dfmc = drill_up(dfm, 'country', 'POSTCODE_COUNTRY', cols_first=None)
    # print('dfmd')
    # print(dfmd)

    # # todo function this
    # dfmd = dfm.groupby('district')[['population', 'pop_male', 'pop_female', 'area_size']].sum().reset_index()
    # dfmd['location_level'] = 'POSTCODE_DISTRICT'
    # dfmd['location'] = dfmd['district']

    # dfma = dfm.groupby('area')[['population', 'pop_male', 'pop_female', 'area_size']].sum().reset_index()
    # dfma['location_level'] = 'POSTCODE_AREA'
    # dfma['location'] = dfma['area']

    # dfmc = dfm.groupby('country')[['population', 'pop_male', 'pop_female', 'area_size']].sum().reset_index()
    # dfmc['location_level'] = 'POSTCODE_AREA'
    # dfmc['location'] = dfmc['area']

    df  = pd.concat([dfm, dfmd, dfma, dfmc])
    print(df)
    # df = dfm

    #          'location', 'sector', 'district', 'area', 'region',  'location_level', 'population', 'area_ha',   'id'
    dfd = dfm[['location', 'sector', 'district', 'area', 'country', 'location_level', 'population', 'area_size']]
    dfd = dfd.set_index('sector', drop=False).T.to_dict('list')
    dfd = {k: tuple(v) for k,v in dfd.items()}


    import dimension
    loc_dim : dimension.LocationDimension = dimension.LocationDimension._from_dict(dfd ,headers= df.columns.tolist())
    print(loc_dim.items()[:10])

    import shared_funcs
    shared_funcs.dicts['location'] = loc_dim

    pass

def etl_old(fpath):
    """Load census data into database as its own table"""

    # process the input file to get a dict of {postcode_sector: total_population}
    population_dict = get_filled_population_dict(fpath)

    # add up sector number to district and area values
    add_up_sectors(population_dict)

    # save to the SQL database
    pop_df = save_to_sql(population_dict)
    # print(pop_df)

# consider doing this before putting all the locations into sql - or just have the census dict be the main location table
def move_to_locations():
    eng = shared_funcs.get_sqlalchemy_con()
    with eng.connect() as con:
        con.execute("alter table location add column population int;")
        con.execute("update location l left join census c on l.sector = c.location set l.population = c.population;")


# call the etl function if this file is run as a stand-alone program
if __name__ == '__main__':
    # etl('other_data/KS101SC.csv')
    etl('other_data/open_postcode_geo_scotland.csv', 'other_data/KS101SC.csv')
