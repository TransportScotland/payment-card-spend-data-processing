import pandas as pd
import shared_funcs 
import time
import sqlalchemy

time1 = time.time()

df = pd.read_csv('generated_data/durations_matrix.csv')

locs = pd.read_sql(
    sql='select location, id from census;', 
    con=sqlalchemy.create_engine(
        'mysql://', 
        creator=shared_funcs.connect_to_db)
        )

loc_to_id = locs.set_index('location').to_dict()
loc_to_id = loc_to_id['id'] # pandas is weird
loc_to_id['origin'] = 'origin' # for mapping later


df = df.rename(columns = {df.columns[0]: 'origin'})

# map to existing sql location ids 
# to hopefully make lookups much faster (ints instead of strings)
df['origin'] = df['origin'].map(loc_to_id)
df.columns = df.columns.map(loc_to_id)

# unpivot
df = df.melt(id_vars = 'origin', var_name='destination', value_name='seconds')
# df = df.rename({'origin': 'origin_id', 'destination': 'destination_id'})

# i didn't like the order the df was in
df = df.sort_values(['origin', 'destination'])

# changing non-floats to NaN so that it can be null in the db
def tryfloat(x):
    try:
        return float(x)
    except (ValueError, TypeError):
        return None
df['seconds'] = df['seconds'].map(tryfloat)
# print any nulls to show they will be in the database like that
print(df.loc[df['seconds'].isna()])
time2 = time.time()

sqlcon = sqlalchemy.create_engine('mysql://', creator=shared_funcs.connect_to_db)
df.to_sql(
    'durations', 
    con=sqlcon, 
    if_exists='replace', 
    index=False, 
    dtype={'destination': sqlalchemy.types.INT, 'origin': sqlalchemy.types.INT, 'seconds': sqlalchemy.types.FLOAT})
with sqlcon.connect() as con:
    con.execute('ALTER TABLE durations ADD PRIMARY KEY (origin, destination);')
time3 = time.time()
print(f'{time2-time1} seconds to load and manipulate, {time3-time2} seconds to SQL.')