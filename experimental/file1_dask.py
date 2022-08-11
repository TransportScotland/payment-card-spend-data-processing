from typing import Iterable
import dask.dataframe as dd
import pandas as pd
import sqlalchemy

pd.set_option('display.max_columns', 20)


dflib = dd
# dflib = pd

def get_time_table(ddf):
    ddf_time = pd.DataFrame({'time_frame': ddf['time_frame'].unique()})
    if dflib == dd:
        ddf_time = dd.from_pandas(ddf_time, chunksize=2000)
    # print(ddf_time)
    ddf_time['year'] = ddf_time['time_frame'].str[0:4]
    ddf_time['month'] = ddf_time['time_frame'].str[4:6]
    # ddf_time = ddf_time.rename_axis('time_id').reset_index()
    ddf_time['time_id'] = ddf_time.index
    # print(ddf_time)
    return ddf_time

def get_location_table(ddf):

    all_locs = list(set(ddf['cardholder_location'].unique()).union(set(ddf['merchant_location'].unique())))
    ddf_loc = pd.DataFrame({'location': all_locs})
    if (dflib == dd):
        ddf_loc = dd.from_pandas(ddf_loc, chunksize=2000)
    ddf_loc['sector'] = ddf_loc['location'] 
    ddf_loc['district'] = ddf_loc['sector'].str[:-2]
    temp = ddf_loc['district'].str.extract('([A-Z]{1,2})[0-9]')
    if dflib == dd:
        # print(temp.compute())
        temp = temp[0]
    ddf_loc['area'] = temp 
    # print(ddf_loc) 

    ddf_loc['location_id'] = ddf_loc.index
    # ddf_loc = ddf_loc.rename_axis('location_id').reset_index()
    return ddf_loc


# def get_category_table(ddf):
#     all_cats = list(set.union(*[set(ddf[col].unique()) for col in ['mcc_rank1', 'mcc_rank2', 'mcc_rank3']]))
#     ddf_cat = pd.DataFrame({'category': all_cats})
#     if (dflib == dd):
#         ddf_cat = dd.from_pandas(ddf_cat, chunksize=2000)

#     ddf_cat['category_id'] = ddf_cat.index
#     return ddf_cat


def get_simple_dim_table(ddf, col = None, original_cols :Iterable = None, new_name = None):
    """
    Expecting either col or original_cols, not both.
    If passing in original_cols, new_name must also be included
    """
    if col is not None:
        if (original_cols is not None):
            raise Exception('Only one of (col, original_cols) must be provided')
        else:
            original_cols = [col]
            if new_name is None:
                new_name = col
    else:
        # col is None
        if original_cols is not None:
            if new_name is not None:
                pass # only passed original_cols and new_name, all good
            else:
                raise Exception('If passing in original_cols, new_name must be supplied as well')
        else:
            raise Exception('Must pass at least one of (col, original_cols)')


    # get a list of all unique values in columns
    # set.union() expects (s1, s2, s3) not ([s1, s2, s3]). Asterisk unwraps the list
    uniques = list(set.union(*[set(ddf[col].unique()) for col in original_cols]))
    ddf_dim = pd.DataFrame({new_name: uniques})
    if (dflib == dd):
        ddf_dim = dd.from_pandas(ddf_dim, chunksize=2000)

    ddf_dim[f'{new_name}_id'] = ddf_dim.index
    return ddf_dim

def etl(file):

    import time
    time0 = time.time()
    ddf = dflib.read_csv(file, dtype={'time_frame': str})

    ddf_time = get_time_table(ddf)
    ddf = ddf.merge(ddf_time[['time_frame', 'time_id']], how='inner', on='time_frame')

    ddf_loc = get_location_table(ddf)
    ddf = ddf.merge(ddf_loc[['location', 'location_id']], how='inner', left_on='cardholder_location', right_on='location'
        ).rename(columns={'location_id': 'cardholder_location_id'}
        ).drop(columns='location')
    ddf = ddf.merge(ddf_loc[['location', 'location_id']], how='inner', left_on='merchant_location', right_on='location'
        ).rename(columns={'location_id': 'merchant_location_id'}
        ).drop(columns='location')

    # ddf_cat = get_category_table(ddf)
    ddf_cat = get_simple_dim_table(ddf, original_cols=['mcc_rank1', 'mcc_rank2', 'mcc_rank3'], new_name='category')
    # print(ddf_cat.compute())
    ddf = ddf.merge(ddf_cat[['category', 'category_id']], how='inner', left_on='mcc_rank1', right_on='category'
        ).rename(columns={'category_id': 'mcc_rank1_id'}
        ).drop(columns='category')
    ddf = ddf.merge(ddf_cat[['category', 'category_id']], how='inner', left_on='mcc_rank2', right_on='category'
        ).rename(columns={'category_id': 'mcc_rank2_id'}
        ).drop(columns='category')
    ddf = ddf.merge(ddf_cat[['category', 'category_id']], how='inner', left_on='mcc_rank3', right_on='category'
        ).rename(columns={'category_id': 'mcc_rank3_id'}
        ).drop(columns='category')

    # also do location_level later

    ddf_chtype = get_simple_dim_table(ddf, 'cardholder_type')
    # print(ddf_chtype.compute())

    ddf = ddf.merge(ddf_chtype, how = 'inner', on='cardholder_type')

    # ddf = ddf[['time_id', 'location_id', ...]]
    ddf = ddf[['time_id', 'cardholder_type_id', 'cardholder_location_id', 'merchant_location_id',
        'pan_cnt', 'txn_cnt', 'txn_gbp_amt', 'mcc_rank1_id', 'mcc_rank2_id', 'mcc_rank3_id']]


    # ddf = ddf[['time_id', 'cardholder_location_id', 'pan_cnt', 'txn_cnt', 'txn_gbp_amt']]
    # df = ddf.compute() if dflib == dd else ddf
    # df = pd.DataFrame(df)


    time1 = time.time()
    print(f'Computation took {time1-time0} seconds.') # 15-18s

    # sql_uri = 'mysql://temp_user:password@localhost/sgov'
    # # dbcon = sqlalchemy.create_engine(sql_uri)
    # time0 = time.time()
    # ddf.to_sql(name='fact1', uri = sql_uri, if_exists='replace', chunksize = 2000)
    # time1 = time.time()
    # print(f'fact1 to_sql took {time1-time0} seconds.')
    # for name, ddf_dim in [
    #     ('time', ddf_time),
    #     ('location', ddf_loc),
    #     ('category', ddf_cat),
    #     ('cardholder_type', ddf_chtype)
    #     ]:
    #     time0 = time.time()
    #     ddf_dim.to_sql(name=name, uri= sql_uri, if_exists='replace', chunksize = 2000)
    #     time1 = time.time()
    #     print(f'{name} to_sql took {time1-time0} seconds')
    # # print(df)



if __name__ == '__main__':
    import time
    time0 = time.time()

    etl('data/file1.csv')
    # etl('data/file1_2k.csv')

    time1 = time.time()
    print(f'Time taken: {time1-time0} seconds.')