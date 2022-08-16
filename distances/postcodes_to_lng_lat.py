import pandas as pd
import dask.dataframe as dd

# copied from etl/census_data.py
def prepare_postcode_info_df(postcode_fpath):
    dfp = pd.read_csv(postcode_fpath, header=None, dtype={7:object, 8:object})
    dfp = dfp[[0,1,2,6,7,8,9,12,13,14]]
    dfp = dfp.rename(columns={0: 'postcode', 1: 'status', 2: 'usertype', 6:'country', 7:'lat', 8:'lng', 
        9:'postcode_no_space', 12: 'area', 13:'district', 14:'sector'})

    dfp['lat'] = pd.to_numeric(dfp['lat'], errors='coerce')
    dfp['lng'] = pd.to_numeric(dfp['lng'], errors='coerce')

    dfp = dfp[dfp['status'] == 'live']
    dfp = dfp[dfp['usertype'] == 'small']


    sector_locs = dfp.groupby('sector')[['lat', 'lng']].mean()
    district_locs = dfp.groupby('district')[['lat', 'lng']].mean()
    area_locs = dfp.groupby('area')[['lat', 'lng']].mean()

    out = pd.concat([sector_locs, district_locs, area_locs])
    return out

def read_trip_csv(path):

    df = dd.read_csv(path, header=0, on_bad_lines='skip')
    return df
    # with pd.read_csv(path, header=0, on_bad_lines='warn', chunksize=10000) as reader:
    #     for df in reader:

    #         return df

dfp = prepare_postcode_info_df('other_data/open_postcode_geo.csv')
print(dfp)
dft = read_trip_csv('generated_data/distinct_trips.csv')
dft = dft.rename(columns= {'cardholder_location': 'origin', 'merchant_location': 'destination'})

print(dft)
# df = dd.from_pandas(dft).merge(dfp, left_on='origin', right_index=True)
df = dft.merge(dfp, left_on='origin', right_index=True) \
    .rename(columns={'lat':'origin_lat', 'lng':'origin_lng'}) \
    .merge(dfp, left_on = 'destination', right_index= True) \
    .rename(columns = {'lat': 'destination_lat', 'lng': 'destination_lng'})

print(df)

df.to_csv('generated_data/distinct_trips_lng_lat.csv', single_file = True, index = False, header = False)
