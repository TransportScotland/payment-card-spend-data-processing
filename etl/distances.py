import pandas as pd
import shared_funcs 
import time
import sqlalchemy

def etl(durations_matrix_files):

    # temporarily getting only the first element of a list of duration files
    if (type(durations_matrix_files) == list):
        durations_matrix_file = durations_matrix_files[0]

    time1 = time.time()

    # load in a durations matrix
    df = pd.read_csv(durations_matrix_file)

    df = df.iloc[:100]

    # load in census data to match location IDs
    locs = pd.read_sql(
        sql='select location, id from census;', 
        con=sqlalchemy.create_engine(
            'mysql://temp_user:password@localhost/sgov', 
            # 'mysql://', 
            # creator=shared_funcs.connect_to_db
            )
        )

    # turn the locations df into a dictionary {location -> location_id} (ids from census table)
    loc_to_id = locs.set_index('location').to_dict() # gets dict like {'id': {'AB10 1':1, 'DD1 4': 2, ...}}
    loc_to_id = loc_to_id['id'] # get the actual column dictionary from what to_dict returns
    loc_to_id['origin'] = 'origin' # for mapping later. one of the columns is called 'origin' and we want to keep that name


    df = df.rename(columns = {df.columns[0]: 'origin'}) # rename the first column to be origin instead of the long description

    # map to existing sql location ids 
    # to hopefully make lookups much faster (ints instead of strings)
    df['origin'] = df['origin'].map(loc_to_id)
    df.columns = df.columns.map(loc_to_id)

    # unpivot
    df = df.melt(id_vars = 'origin', var_name='destination', value_name='seconds')

    # i didn't like the order the df was in
    df = df.sort_values(['origin', 'destination'])

    df = df.rename(columns={'origin': 'origin_id_census', 'destination': 'destination_id_census'})

    # changing non-floats to NaN so that it becomes in the db
    def tryfloat(x):
        try:
            return float(x)
        except (ValueError, TypeError):
            return None
    df['seconds'] = df['seconds'].map(tryfloat)
    # print any nulls to show they will be in the database like that
    print(df.loc[df['seconds'].isna()])
    time2 = time.time()

    print(df)
    sqlcon = sqlalchemy.create_engine('mysql://', creator=shared_funcs.connect_to_db)
    df.to_sql(
        'durations', 
        con=sqlcon, 
        if_exists='replace', 
        index=False, 
        dtype={'destination_id_census': sqlalchemy.types.INT, 'origin_id_census': sqlalchemy.types.INT, 'seconds': sqlalchemy.types.FLOAT})
    with sqlcon.connect() as con:
        # con.execute('ALTER TABLE durations ADD PRIMARY KEY (origin, destination);')
        con.execute('ALTER TABLE durations ADD PRIMARY KEY (origin_id_census, destination_id_census);')

        time3 = time.time()
        secp1 = df['seconds'] + 1
        df['secp1'] = secp1
        df2 = df[['origin_id_census', 'destination_id_census', 'secp1']]
        df.drop('secp1', 1)

        df2.to_sql(
            'dur_temp', 
            con=sqlcon, 
            if_exists='replace', 
            index=False, 
            dtype={'destination_id_census': sqlalchemy.types.INT, 'origin_id_census': sqlalchemy.types.INT, 'secp1': sqlalchemy.types.FLOAT}
            )

        con.execute("ALTER TABLE durations ADD COLUMN secp1 INT;")
        con.execute("""UPDATE durations 
            INNER JOIN dur_temp ON 
                durations.origin_id_census = dur_temp.origin_id_census
                AND durations.destination_id_census = dur_temp.destination_id_census
            SET durations.secp1 = dur_temp.secp1
            ;""")
        con.execute("DROP TABLE IF EXISTS dur_temp;")


    time4 = time.time()
    print(f'{time2-time1} seconds to load and manipulate, {time3-time2} seconds to SQL, {time4-time3} to add sql column.')

if __name__ == '__main__':
    import sys
    etl(sys.argv[1])