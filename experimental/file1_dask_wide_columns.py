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

def two_columns_to_series(ddf, index_col, values_col):
# times with 100k rows and only time and location
# the speeds literally don't make a difference I am doing these calculations only like five times
    # return dflib.Series(ddf[values_col].values, ddf[index_col]) # only works on pandas. May still be faster bc small enough
    # return ddf.set_index(index_col)[values_col] # 0.77s
    # return dd.from_pandas(pd.Series(ddf[values_col].compute().values, ddf[index_col].compute()), chunksize = 2000) # 0.76s
    # return pd.Series(ddf[values_col].compute().values, ddf[index_col].compute()) # 0.80s
    df = ddf[[index_col, values_col]].compute()
    # return dd.from_pandas(pd.Series(df[index_col].values, df[values_col]), chunksize=500) # slower
    return pd.Series(df[index_col].values, df[values_col]) # 0.78s

# # copied from dask source code for now
# def _to_sql_chunk(d, uri, **kwargs):
#     import sqlalchemy as sa

#     engine = sa.create_engine(uri)

#     # q = d.to_sql(con=engine, **kwargs)
#     print(d)
#     engine.dispose()

#     # return q


# def my_to_sql(ddf, name, uri, if_exists = 'replace'):

#     from dask.delayed import delayed, tokenize
#     from dask.base import compute as dask_compute
#     # Partitions should always append to the empty table created from `meta` above
#     _to_sql_chunk(ddf._meta, uri, name=name, if_exists = if_exists)
#     worker_kwargs = dict(if_exists="append", name=name)
#     result = [
#         _to_sql_chunk(
#             d, uri,
#             **worker_kwargs,
#             dask_key_name="to_sql-%s" % tokenize(d, **worker_kwargs),
#         )
#         for d in ddf.to_delayed()
#     ]
#     result = delayed(result)

#     dask_compute(result)
    
#     pass


# def save_batch(batch, dbcon, table_name):
#     values = [batch.popleft() for _ in range(len(batch))]

#     # headers = table_info.headers_dict[table_name]
#     # headers = 
#     headers_str = '('+ ','.join(headers) + ')'
#     values_f_str = '(' + ','.join(['%s' for _ in range(len(headers))]) + ')'
#     values_f_str = '' # constructing a full command instead of parameters

#     # dbcon.cursor().executemany(sql, values)

#     # may need to escape if anything other than numbers comes in.
#     # this is where it would've been useful to have types
#     # values_str = ','.join(['('+','.join([dbcon.escape_string(str(e)).decode('ascii') for e in v]) + ')' for v in values])

#     values_str = ','.join([str(v) for v in values])
#     bigsql = sql + values_str
#     dbcon.cursor().execute(bigsql)

def my_to_sql(ddf, name, uri, if_exists):
    dbcon = sqlalchemy.create_engine(uri)
    # i = 0
    for i in range(ddf.npartitions):
        partition = ddf.get_partition(i)
        df = partition.compute()
        # print(df)
        headers = list(df.columns)
        values_d = df.to_dict()
        headers_str = '('+ ','.join(headers) + ')'
        sql_base = f"INSERT INTO {name} {headers_str} VALUES "

        zipped_vals = zip(*[values_d[col] for col in headers])
        values_str = ','.join([str(v) for v in zipped_vals])
        bigsql = sql_base + values_str
        dbcon.execute(bigsql)
        

        # assert(all(type(e) for e in (int, float) for row in values for e in row))
        # if i == 0:
        #     # partition.to_sql('CDR_PBI_Report', uri=to_sql_uri, if_exists='replace', index=False)
        # if i > 0:
        #     # partition.to_sql('CDR_PBI_Report', uri=to_sql_uri, if_exists='append', index=False)
        # i += 1

def add_time_cols(ddf: dd.DataFrame):
    # ddf_time = pd.DataFrame({'time_frame': ddf['time_frame'].unique()})
    # if dflib == dd:
    #     ddf_time = dd.from_pandas(ddf_time, chunksize=2000)
    # print(ddf_time)

    # quarter_cond = ddf['time_frame'].str.len() > 8
    # month_cond = ddf['time_frame'].str.len() <= 8
    # ddf_qs = ddf.loc[ddf['time_frame'].str.len() > 8, :]
    # ddf_ms = ddf.loc[ddf['time_frame'].str.len() <= 8, :]
    # ddf.loc[month_cond, 'year']  = ddf.loc[month_cond, 'time_frame'].str[0:4]
    # ddf.loc[month_cond, 'month'] = ddf.loc[month_cond, 'time_frame'].str[4:6]
    # ddf.loc[month_cond,'quarter'] = ddf.loc[month_cond, 'month'].map({
    #     m: int((m-1)/3)+1 for m in range(1,13)
    # })

    def month_slice(x):
        return x.loc[x['time_frame'].str.len() <= 8, 'time_frame']
    def quarter_slice(x):
        return x.loc[x['time_frame'].str.len() > 8, 'time_frame']



    print(type(ddf))
    ddf['year'] = ddf.map_partitions(lambda x: month_slice(x).str[0:4])
    ddf['month'] = ddf.map_partitions(lambda x: month_slice(x).str[4:6]).astype('Int8')
    ddf['quarter'] = ddf.map_partitions(lambda x: month_slice(x).map({
        m: int((m-1)/3)+1 for m in range(1,13)
    }))
    

    # import numpy as np
    # ddf['year'] = ddf.map_partitions(lambda x: quarter_slice(x).str[4:6])
    ddf['year'] = ddf['year'].mask(ddf.year.isna(), '20' + ddf['time_frame'].str[4:6])

    # ddf['year']  = ddf['time_frame'].where(~ month_cond, ddf['time_frame'].str[0:4])
    # ddf['month'] = ddf['time_frame'].where(~ month_cond, ddf['time_frame'].str[4:6])
    # ddf['quarter'] = ddf['time_frame'].where(~ month_cond, {
    #     m: int((m-1)/3)+1 for m in range(1,13)
    # })

    # ddf_ms['quarter'] = ((ddf_ms['month'] -1 ) / 3).astype(int) + 1

    # ddf_qs.loc[:,'quarter'] = ddf_ms.loc[:, 'time_frame'].str[:3].map({
    #     'Jan': 1, 'Apr': 2, 'Jul': 3, 'Oct': 4
    # })
    # ddf_qs.loc[:, 'year'] = '20' + ddf_qs.loc[:,'time_frame'].str[4:6]
    # ddf['year'] = ddf['time_frame'].where (~ quarter_cond, '20' + ddf['time_frame'].str[4:6])
    # ddf['quarter'] = ddf['time_frame'].where(~ quarter_cond, {
    #     'Jan': 1, 'Apr': 2, 'Jul': 3, 'Oct': 4
    # })
    
    # ddf = ddf.rename_axis('time_id').reset_index()
    # ddf['month'] = ddf['month'].astype('Int8')
    # ddf['quarter'] = ddf['quarter'].astype('Int8')

    # ddf = dflib.concat([ddf_ms, ddf_qs])
    return ddf

dflib = pd

def load(filepath : str) -> dd.DataFrame:
    return dd.read_csv(filepath, dtype={'time_frame': str}, sep = '|')
    pass

def add_columns(ddf) -> dd.DataFrame:
    ddf = add_time_cols(ddf)
    return ddf
    pass

def save_pq(ddf, filepath : str):
    pass

def pq_to_db(filepath :str):
    
    pass

def etl(file : str):
    ddf :dd.DataFrame = load(file)
    print(ddf)
    ddf = add_columns(ddf)
    print(ddf.compute())
    df: pd.DataFrame = ddf.compute()
    print(df.loc[df['year'] != '2021', :])
    pqfname = f'{file}.parquet'
    save_pq(ddf, pqfname)
    pq_to_db(pqfname)


def etl_old(file):

    import time
    time0 = time.time()
    ddf = dflib.read_csv(file, dtype={'time_frame': str})

    ddf_time = get_time_table(ddf)
    # ddf = ddf.merge(ddf_time[['time_frame', 'time_id']], how='inner', on='time_frame')
    ddf['time_id'] = ddf['time_frame'].map(two_columns_to_series(ddf_time, 'time_id', 'time_frame'))

    ddf_loc = get_location_table(ddf)
    map_series_loc = two_columns_to_series(ddf_loc, 'location_id', 'location')
    ddf['cardholder_location_id'] = ddf['cardholder_location'].map(map_series_loc)
    ddf['merchant_location_id'] = ddf['merchant_location'].map(map_series_loc)
    # ddf = ddf.merge(ddf_loc[['location', 'location_id']], how='inner', left_on='cardholder_location', right_on='location'
    #     ).rename(columns={'location_id': 'cardholder_location_id'}
    #     ).drop(columns='location')
    # ddf = ddf.merge(ddf_loc[['location', 'location_id']], how='inner', left_on='merchant_location', right_on='location'
    #     ).rename(columns={'location_id': 'merchant_location_id'}
    #     ).drop(columns='location')

    # ddf_cat = get_category_table(ddf)

    ddf_cat = get_simple_dim_table(ddf, original_cols=['mcc_rank1', 'mcc_rank2', 'mcc_rank3'], new_name='category')
    # # print(ddf_cat.compute())
    map_series_cat = two_columns_to_series(ddf_cat, 'category_id', 'category')
    ddf['mcc_rank1_id'] = ddf['mcc_rank1'].map(map_series_cat)
    ddf['mcc_rank2_id'] = ddf['mcc_rank2'].map(map_series_cat)
    ddf['mcc_rank3_id'] = ddf['mcc_rank3'].map(map_series_cat)
    # ddf = ddf.merge(ddf_cat[['category', 'category_id']], how='inner', left_on='mcc_rank1', right_on='category'
    #     ).rename(columns={'category_id': 'mcc_rank1_id'}
    #     ).drop(columns='category')
    # ddf = ddf.merge(ddf_cat[['category', 'category_id']], how='inner', left_on='mcc_rank2', right_on='category'
    #     ).rename(columns={'category_id': 'mcc_rank2_id'}
    #     ).drop(columns='category')
    # ddf = ddf.merge(ddf_cat[['category', 'category_id']], how='inner', left_on='mcc_rank3', right_on='category'
    #     ).rename(columns={'category_id': 'mcc_rank3_id'}
    #     ).drop(columns='category')

    # # also do location_level later

    ddf_chtype = get_simple_dim_table(ddf, 'cardholder_type')
    # # print(ddf_chtype.compute())
    ddf['cardholder_type_id'] = ddf['cardholder_type'].map(two_columns_to_series(ddf_chtype, 'cardholder_type_id', 'cardholder_type'))
    # ddf = ddf.merge(ddf_chtype, how = 'inner', on='cardholder_type')

    # ddf = ddf[['time_id', 'location_id', ...]]
    ddf = ddf[['time_id', 'cardholder_type_id', 'cardholder_location_id', 'merchant_location_id',
        'pan_cnt', 'txn_cnt', 'txn_gbp_amt', 'mcc_rank1_id', 'mcc_rank2_id', 'mcc_rank3_id']]


    # ddf = ddf[['time_id', 'cardholder_location_id', 'merchant_location_id', 'pan_cnt', 'txn_cnt', 'txn_gbp_amt']]
    # df = ddf.compute() if dflib == dd else ddf
    # df = pd.DataFrame(df)

    sql_uri = 'mysql://temp_user:password@localhost/sgov'
    # dbcon = sqlalchemy.create_engine(sql_uri)

    time1 = time.time()
    print(f'Computation took {time1-time0} seconds.') #10-11s
    time0 = time.time()


    # ddf.to_sql(name='fact1', uri = sql_uri, if_exists='replace') # 3.41s on 100k, 36s on 1m
    # ddf.to_sql(name='fact1', uri = sql_uri, if_exists='replace', chunksize = 2000) # 3.41s on 100k
    # ddf.to_sql(name='fact1', uri = 'mysql+mysqldb://temp_user:password@localhost/sgov', if_exists='replace', ) # 3.5s
    # ddf.to_sql(name='fact1', uri = sql_uri, if_exists='replace', parallel=True) # 3.9s on 100k
    # ddf.to_sql(name='fact1', uri = sql_uri, if_exists='replace', parallel=True, chunksize= 2000) # 3.4s on 100k,
    ddf.to_sql(name='fact1', uri = sql_uri, if_exists='replace', parallel=True, chunksize= 1000) # 3.4s on 100k, 24s on 1m
    # ddf.to_sql(name='fact1', uri = sql_uri, if_exists='replace', parallel=True, chunksize= 500) # 3.5s on 100k
    # my_to_sql(ddf, name='fact1', uri = sql_uri, if_exists='replace') # 2.26s on 100k, 19s on 1m
    time1 = time.time()
    print(f'fact1 to_sql took {time1-time0} seconds.')
    for name, ddf_dim in [
        ('time', ddf_time),
        ('location', ddf_loc),
        ('category', ddf_cat),
        ('cardholder_type', ddf_chtype)
        ]:
        time0 = time.time()
        ddf_dim.to_sql(name=name, uri= sql_uri, if_exists='replace', chunksize = 2000)
        time1 = time.time()
        print(f'{name} to_sql took {time1-time0} seconds')
    # print(df)



if __name__ == '__main__':
    import time
    time0 = time.time()

    etl('data/module1_sample.csv')
    # etl('data/file1_2k.csv')
    # etl('data/file1_1e5.csv')

    time1 = time.time()
    print(f'Time taken: {time1-time0} seconds.')