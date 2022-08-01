import pandas as pd
import shared_funcs 
import time
import sqlalchemy

def etl(distances_matrix_files):
    """Load the travel distance matrix into a SQL database, un-pivoted from wide format to long format, with IDs matching those in census table"""

    # temporarily getting only the first element of a list of duration files. in the future will hopefully be able to load multiple at once
    if (type(distances_matrix_files) == list):
        distances_matrix_file = distances_matrix_files[0]

    time1 = time.time()

    # load in a durations matrix
    df = pd.read_csv(distances_matrix_file)

    # df = df.iloc[:100] # uncommment to only take the first 100 elements to speed up testing

    # load in census data to match location IDs
    # TODO consider loading location table instead (a subset of census table)
    locs = pd.read_sql(
        sql='select location, id from census;', 
        con=sqlalchemy.create_engine(
            'mysql://temp_user:password@localhost/sgov', 
            )
        )

    # turn the locations df into a dictionary {location -> location_id_from_census_table}
    loc_to_id : dict = locs.set_index('location').to_dict() # gets dict like {'id': {'AB10 1':1, 'DD1 4': 2, ...}}
    loc_to_id = loc_to_id['id'] # get the actual column dictionary from what df.to_dict returns
    loc_to_id['origin'] = 'origin' # for mapping later. one of the columns is called 'origin' and we want to keep that name


    df = df.rename(columns = {df.columns[0]: 'origin'}) # rename the first column to be origin instead of the long description

    # map to existing sql location ids 
    # to hopefully make later lookups a lot faster (ints instead of strings)
    df['origin'] = df['origin'].map(loc_to_id)
    df.columns = df.columns.map(loc_to_id)

    # unpivot wide to long format
    df = df.melt(id_vars = 'origin', var_name='destination', value_name='seconds')

    # melt returns it sorted by destination first and then origin, like [(AB10: AB10), (AB11:AB10) ... (ZE9: AB10),, (AB10: AB11), ...]
    # so changing it here because it feels more right
    df = df.sort_values(['origin', 'destination'])

    # rename to be more descriptive
    df = df.rename(columns={'origin': 'origin_id_census', 'destination': 'destination_id_census'})

    # changing non-floats to NaN so that it becomes null in the db rather than trying to insert a bad string
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
    
    # save to database
    sqlcon = sqlalchemy.create_engine('mysql://', creator=shared_funcs.connect_to_db)
    df.to_sql(
        'durations', 
        con=sqlcon, 
        if_exists='replace', 
        index=False, 
        dtype={'destination_id_census': sqlalchemy.types.INT, 'origin_id_census': sqlalchemy.types.INT, 'seconds': sqlalchemy.types.FLOAT})
    with sqlcon.connect() as con:
        # add a primary key on the origin->destination column pair
        con.execute('ALTER TABLE durations ADD PRIMARY KEY (origin_id_census, destination_id_census);')



    time3 = time.time()
    print(f'{time2-time1} seconds to load and manipulate, {time3-time2} seconds to load into SQL database.')

if __name__ == '__main__':
    # todo have a default filename or an error message
    import sys
    etl(sys.argv[1])