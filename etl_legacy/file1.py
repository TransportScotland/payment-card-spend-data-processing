import MySQLdb
import shared_funcs


# fact1_headers = ('pan_cnt', 'txn_cnt', 'txn_gbp_amt', 'time_id', 'cardholder_id', 'merchant_id', 'cat1_id', 'cat2_id', 'cat3_id')
def create_dims(row, skip_bigger_area_rows = True):
    """
    Turn a row tuple from the File 1 csv into a fact table entry, 
    filling up intermediate dimension tables along the way.

    Parameters:
        row: tuple/list indexable by numerical position in the File1 CSV file

    Returns:
        A row of the fact table as a tuple. Also may modify the dimension data structures
    """
    # add dimension data to dimension data structures if not already in, and get the relevant ID
    time_id = shared_funcs.handle_time(row)
    cardholder_id = shared_funcs.handle_loc(row, 2, 3, skip_bigger_area_rows=skip_bigger_area_rows)
    merchant_id = shared_funcs.handle_loc(row, 4, 5, skip_bigger_area_rows=skip_bigger_area_rows)

    # todo handle this differently
    # loc returns -1 if something else than POSTCODE_SECTOR was in the row.
    # currently raising an exception to skip this row - not the right way to do things.
    
    # get measures straight from the row list
    pan_cnt = row[6]
    txn_cnt = row[7]
    txn_gbp_amt = row[8]

    # dimensions again
    # equivalent to: cat1_id, cat2_id, cat3_id = (shared_funcs.handle_one_dim('category', row, i) for i in [9, 10, 11])
    cat1_id = shared_funcs.handle_one_dim('category', row, 9)
    cat2_id = shared_funcs.handle_one_dim('category', row, 10)
    cat3_id = shared_funcs.handle_one_dim('category', row, 11)


    # prepare a tuple of the data to be saved into the fact table.
    # currently this must be in this order because that is how it is set up in the database
    # todo consider turning this into a class/struct with the named variables and a function like get_ordered_tuple()
    fact_row = (pan_cnt, txn_cnt, txn_gbp_amt, time_id, cardholder_id, merchant_id, cat1_id, cat2_id, cat3_id)


    # if cardholder_id == -1 or merchant_id == -1:
    #     # currently a DistrictException is being raised instead
    #     pass

    return fact_row


def etl(in_path, fact_table_name = 'fact1'):
    """
    Starts the ETL process of File 1 of the card data.

    Parameters:
        in_path: File path to the input CSV file
        fact_table_name: (optional) name of the fact table in the database. (May not work correctly)
    """

    # ensure dimensions are set up and ready to accept data from this file
    shared_funcs.ensure_dim_dicts('time', 'location', 'category')

    # process the file, calling create_dims() on each row
    shared_funcs.batch_process(in_path, create_dims, fact_table_name, skip_bigger_area_rows = False)


def get_sums_with_matching(con, cardholder, merchant, time):
    """
    UNSAFE SQL DO NOT EVER LET USER PUT DATA IN HERE.
    cardholder, merchant, time at [1] are okay, but at [0] must always be string literals written in code and not read from a file.
    Once Python 3.11 comes around, look at adding a type hint of typing.StringLiteral
    """
    c = con.cursor()
    c.execute(
        f"select sum(pan_cnt), sum(txn_cnt), sum(txn_gbp_amt) from \
            fact1 f \
                inner join location cl on f.cardholder_id = cl.id \
                inner join location ml on f.merchant_id   = ml.id \
                inner join time t on f.time_id = t.id \
            where cl.{cardholder[0]} = %s \
            and ml.{merchant[0]} = %s \
            and t.{time[0]} = %s \
            ;",
        ((cardholder[1],), (merchant[1],), (time[1],)) # turn strings into tuples os that string
    )
    return c.fetchone()

# TODO handle non-sector data at cardholder level and area/region data at other levels
# TODO consider fetching rows where sum(measure) is NULL separately from rows whre sum(measure) returns something, 
# to save (likely) a lot of time from SQL parsing (sending two bigger requests instead of thousands of smaller ones).
# Or if sector-sector data is rare enough, may even be better to handle those as exceptions than handling districts as exceptions
# TODO district_rows shouldn't be in-memory, they won't fit
def district_row_generator_modified(district_rows, con):
    """
    Generator for modified rows ready to go into create_dims.
    Gets the sums of individual measures where district matches, 
    and subtracts it from the original measure values.
    Also adds _DEAGG to location_level to make it identifiable
    and adds an ' X' to a district to make it compatible with sector splitting.
    """
    for row in district_rows:
        row = list(row) 
        origin_sector = row[3] # todo make these dynamic based on [2],[4]
        dest_district = row[5]
        time = row[0]

        tup = get_sums_with_matching(con, ('sector', origin_sector), ('district', dest_district), ('raw',time))
        tup = tuple(t if t else 0 for t in tup) # changes None values to 0s

        row[6] = int(int(row[6]) - tup[0])
        row[7] = int(int(row[7]) - tup[1])
        row[8] = float(float(row[8]) - tup[2])
        yield row


def fix_districts():
    """
    Goes through rows in shared_funcs.district_rows and saves them to database
    after running them through district_row_generator_modified
    """
    if shared_funcs.district_rows:
        con = shared_funcs.connect_to_db()
        shared_funcs.batch_process_threaded_from_generator(
            district_row_generator_modified(
                shared_funcs.district_rows, con),
                create_dims, 'fact1', con, if_exists='append', skip_bigger_area_rows = False)

