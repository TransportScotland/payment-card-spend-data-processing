

import pandas as pd
import dask.dataframe as dd

def prepare_postcode_info_df(postcode_fpath):
    dfp = dd.read_csv(postcode_fpath, header=None, dtype={7: object, 8:object, 3:object, 4:object})
    dfp = dfp[[0,1,2,6,7,8,9,12,13,14]]
    dfp = dfp.rename(columns={0: 'postcode', 1: 'status', 2: 'usertype', 6:'country', 7:'lat', 8:'lng', 
        9:'postcode_no_space', 12: 'area', 13:'district', 14:'sector'})

    dfp = dfp[dfp['status'] == 'live']
    dfp = dfp[dfp['usertype'] == 'small']

    dfp['lat'] = dd.to_numeric(dfp['lat'], errors='coerce')
    dfp['lng'] = dd.to_numeric(dfp['lat'], errors='coerce')

    avg_locs = dfp.groupby('sector')[['lat', 'lng']].mean()

    dfp = dfp.groupby('sector')[['status', 'country', 'area', 'district']].first()
    dfp = dfp.merge(avg_locs, left_index=True, right_index=True)
    dfp = dfp.reset_index()

    dfp_pandas = dfp.compute()
    return dfp_pandas


def prepare_scotland_census_df(census_fpath):

    dfc = pd.read_csv(census_fpath, header=None, skiprows=6, thousands=',')
    dfc = dfc[:-4]
    dfc = dfc[[0,1,2,3,7]]
    dfc = dfc.rename(columns={0: 'sector', 1:'population', 2:'pop_male', 3: 'pop_female', 7: 'area_size'})
    #will calculate density manually because some postcode sectors are split into  parts

    dfc = dfc.convert_dtypes() # make it stop converting ints to object by converting 'int' to 'Int64' (idk why pandas is this way)
    dfc['sector'] = dfc['sector'].str.extract(r'(\b\w\w?\d[\d\w]? \d)') # start of word, 1 or 2 letters, a digit, a digit or a letter, a space, and a digit
    dfc = dfc.groupby('sector')[['population', 'pop_male', 'pop_female', 'area_size']].sum()
    dfc = dfc.reset_index()
    return dfc

def prepare_eng_wal_census_df(census_fpaths):
    dfcs = [pd.read_csv(path, header=None, skiprows=1) for path in census_fpaths]
    dfc = pd.concat(dfcs)
    dfc = dfc.rename(columns={0: 'postcode', 1:'population', 2: 'pop_male', 3: 'pop_female'})

    dfc = dfc.convert_dtypes()
    dfc['sector'] = dfc['postcode'].str[:-2]
    # change AB102 to AB10 2
    dfc = dfc.groupby('sector')[['population', 'pop_male', 'pop_female']].sum()
    dfc = dfc.reset_index()
    # add missing spaces
    dfc.loc[dfc['sector'].str[3] != ' ', 'sector'] = dfc['sector'].str[:4] + ' ' + dfc['sector'].str[-1]
    # replace multiple spaces with one space
    dfc['sector'] = dfc['sector'].str.replace('  ', ' ', regex=False) 
    return dfc

def assign_proportional_numbers_ni(df1: pd.DataFrame, df1_end :pd.DataFrame):
    # return df1
    pass
    rows = df1_end.to_numpy().tolist()[4:8]
    strs = [row[0] for row in rows]
    num_strs = [s.split(':')[1].replace(',', '') for s in strs]
    num_ints = [int(num) for num in num_strs]

    unassigned_total, unassigned_m, unassigned_f, _ = num_ints

    count_stars = (df1['population'] == '*').to_list().count(True)
    count_na= df1['population'].isna().to_list().count(True)
    count_invalid = count_stars + count_na

    val_t = unassigned_total / count_invalid
    val_m = unassigned_m / count_invalid
    val_f = unassigned_f / count_invalid

    df1.loc[df1['population'] == '*', 'population'] = val_t
    df1.loc[df1['population'] == '*', 'pop_male'] = val_m
    df1.loc[df1['population'] == '*', 'pop_female'] = val_f
    df1.loc[df1['population'].isna(), 'population'] = val_t
    df1.loc[df1['population'].isna(), 'pop_male'] = val_m
    df1.loc[df1['population'].isna(), 'pop_female'] = val_f
    return df1

def prepare_ni_census_df(table1_fpath, table2_fpath):
    # NI census is different yet again, it shows totals for supressed districts in a separate table
    df1 = pd.read_csv(table1_fpath, header=None, skiprows=6)#, skipfooter=9)
    df1_end = df1[-9:]
    df1 = df1[:-9]
    df1 = df1.rename(columns= {0: 'postcode', 1:'population', 2: 'pop_male', 3: 'pop_female'})

    df1 = assign_proportional_numbers_ni(df1, df1_end)

    for col in ['population', 'pop_male', 'pop_female']:
        df1[col] = pd.to_numeric(df1[col], errors='coerce')

    df1['sector'] = df1['postcode'].str[:-2]
    df1 = df1.groupby('sector')[['population', 'pop_male', 'pop_female']].sum()
    df1 = df1.reset_index()
    # add missing spaces
    df1.loc[df1['sector'].str[3] != ' ', 'sector'] = df1['sector'].str[:4] + ' ' + df1['sector'].str[-1]
    # replace multiple spaces with one space
    df1['sector'] = df1['sector'].str.replace('  ', ' ', regex=False) 
    return df1

    # df2 = pd.read_csv(table2_fpath)
    # print(df2)
    # pass


def collect_locations(dfm: pd.DataFrame):

    # to prevent modifying the original df
    dfms = dfm[dfm.columns.to_list()]

    dfms['location_level'] = 'POSTCODE_SECTOR'
    dfms['location'] = dfms['sector']
    
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
        return dfo

    dfmd = drill_up(dfm, 'district', 'POSTCODE_DISTRICT', cols_first=['area', 'country'])
    dfma = drill_up(dfm, 'area', 'POSTCODE_AREA', cols_first=['country'])
    dfmc = drill_up(dfm, 'country', 'POSTCODE_COUNTRY', cols_first=None)
    df  = pd.concat([dfms, dfmd, dfma, dfmc])

    df[['population', 'pop_male', 'pop_female']] = df[['population', 'pop_male', 'pop_female']].round(0).astype('Int64')

    return df

def get_density(df):
    return df['population'] / df['area_size']

def to_dimension(df):
    #         'location', 'sector', 'district', 'area', 'region',  'location_level', 'population', 'area_ha',   'id'
    cols = ['location', 'sector', 'district', 'area', 'country', 'location_level', 
        'lat', 'lng','population', 'area_size']
    dfd = df[cols]
    import numpy as np
    dfd = dfd.replace(to_replace=np.nan, value=None) # replace pandas NaNs with NoneType Nones, so they can be NULL in the db
    dfl = dfd.to_numpy().tolist()
    dfd = {row[0]: tuple(row) for row in dfl}

    import dimension
    loc_dim : dimension.LocationDimension = dimension.LocationDimension._from_dict(dfd ,headers= cols)
    return loc_dim


def etl(postcode_fpath, census_scotland_fpath, census_eng_wal_fpaths, census_ni_fpaths):
    # might actually be better using pandas here than my own solution because I'm merging two tables
    import pandas as pd
    
    dfp = prepare_postcode_info_df(postcode_fpath)
    dfc_scot = prepare_scotland_census_df(census_scotland_fpath)
    dfc_engw = prepare_eng_wal_census_df(census_eng_wal_fpaths)
    dfc_ni = prepare_ni_census_df(census_ni_fpaths[0], census_ni_fpaths[1])
    dfc = pd.concat([dfc_scot, dfc_engw, dfc_ni])
    dfm = dd.merge(dfp, dfc, left_on='sector', right_on='sector', how='outer')

    df = collect_locations(dfm)

    # df['density'] = get_density(df)

    loc_dim = to_dimension(df)

    import shared_funcs
    shared_funcs.dicts['location'] = loc_dim

    pass

# call the etl function if this file is run as a stand-alone program
if __name__ == '__main__':

    engw_census_fpaths = [f'other_data/Postcode_Estimates_1_{letters}.csv' for letters in ['A_F', 'G_L', 'M_R', 'S_Z']]
    ni_census_fpaths = ['other_data/ni_census_table1.csv', 'other_data/ni_census_table2.csv']
    etl('other_data/open_postcode_geo.csv', 'other_data/KS101SC.csv', engw_census_fpaths, ni_census_fpaths)
