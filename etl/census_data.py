

import pandas as pd

def prepare_postcode_info_df(postcode_fpath):
    dfp = pd.read_csv(postcode_fpath, header=None)
    dfp = dfp[[0,1,2,6,7,8,9,12,13,14]]
    dfp = dfp.rename(columns={0: 'postcode', 1: 'status', 2: 'usertype', 6:'country', 7:'lat', 8:'lng', 
        9:'postcode_no_space', 12: 'area', 13:'district', 14:'sector'})

    dfp = dfp[dfp['status'] == 'live']
    dfp = dfp[dfp['usertype'] == 'small']


    avg_locs = dfp.groupby('sector')[['lat', 'lng']].mean()

    dfp = dfp.drop_duplicates(subset='sector', keep='first')
    dfp = dfp[['status', 'country', 'area', 'district', 'sector']]
    dfp = dfp.merge(avg_locs, left_on='sector', right_index=True)
    return dfp


def prepare_census_df(census_fpath):

    dfc = pd.read_csv(census_fpath, header=None, skiprows=6, thousands=',')
    dfc = dfc[:-4]
    dfc = dfc[[0,1,2,3,7]]
    dfc = dfc.rename(columns={0: 'sector', 1:'population', 2:'pop_male', 3: 'pop_female', 7: 'area_size'})
    #will calculate density manually because some postcode sectors are split into  parts

    dfc = dfc.convert_dtypes() # make it stop converting ints to object by converting 'int' to 'Int64'
    dfc['sector'] = dfc['sector'].str.extract(r'(\b\w\w?\d[\d\w]? \d)')
    dfc = dfc.groupby('sector')[['population', 'pop_male', 'pop_female', 'area_size']].sum()
    dfc = dfc.reset_index()
    return dfc


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

    return df


def get_density(df):
    return df['population'] / df['area_size']

def to_dimension(df):
    #         'location', 'sector', 'district', 'area', 'region',  'location_level', 'population', 'area_ha',   'id'
    dfd = df[['location', 'sector', 'district', 'area', 'country', 'location_level', 
        'lat', 'lng','population', 'area_size', 'density']]
    dfd = dfd.set_index('location', drop=False).T
    import numpy as np
    dfd = dfd.replace(to_replace=np.nan, value=None) # replace pandas NaNs with NoneType Nones, so they can be NULL in the db
    dfd = dfd.to_dict('list')
    dfd = {k: tuple(v) for k,v in dfd.items()}

    import dimension
    loc_dim : dimension.LocationDimension = dimension.LocationDimension._from_dict(dfd ,headers= df.columns.tolist())
    return loc_dim


def etl(postcode_fpath, census_fpath):
    # might actually be better using pandas here than my own solution because I'm merging two tables
    import pandas as pd
    
    dfp = prepare_postcode_info_df(postcode_fpath)
    dfc = prepare_census_df(census_fpath)
    dfm = pd.merge(dfp, dfc, left_on='sector', right_on='sector', how='outer')

    df = collect_locations(dfm)

    df['density'] = get_density(df)

    loc_dim = to_dimension(df)

    import shared_funcs
    shared_funcs.dicts['location'] = loc_dim

    pass

# call the etl function if this file is run as a stand-alone program
if __name__ == '__main__':
    # etl('other_data/KS101SC.csv')
    etl('other_data/open_postcode_geo_scotland.csv', 'other_data/KS101SC.csv')
