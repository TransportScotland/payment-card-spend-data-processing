

ALLOWED_TABLES = {'fact1', 'fact2', 'fact3', 'fact4', 
    'time', 'location', 'category', 'm_channel', 'purpose', 'transport_mode',
    'census'
    }


# maybe have this as classes/structs instead.

headers_dict = {
    'fact1': ('pan_cnt', 'txn_cnt', 'txn_gbp_amt', 'time_id', 'cardholder_id', 
        'merchant_id', 'cat1_id', 'cat2_id', 'cat3_id'),
    'fact2': ('pan_cnt', 'txn_cnt', 'txn_gbp_amt', 'merchant_outlet_count', 
        'percent_repeat',  'time_id', 'cardholder_id', 'category_id', 'm_channel_id'),
    'fact3': ('perc_rail', 'pan_cnt', 'txn_cnt', 'txn_gbp_amt', 
        'time_id', 'merchant_id', 'jour_purpose_id'),
    'fact4': ('perc_jour', 'perc_pan', 
        'time_id', 'merchant_id', 'transport_mode_id'),
    'time': ('raw', 'year', 'quarter', 'month', 'id'),
    'location': ('location', 'sector', 'district', 'area', 'region', 'location_level',
        'latitude', 'longitude', 'population', 'area_size', 'id'),
    'category': ('category', 'id'),
    'm_channel': ('m_channel', 'id'),
    'purpose': ('purpose', 'id'),
    'transport_mode': ('transport_mode', 'id'),
    'census': ('location', 'population', 'id'),
}


create_strings = {}

rdbms = 'mysql'
if rdbms == 'mysql':

    create_strings['fact1'] = ( 
        "CREATE TABLE fact1 ("
        "id INT UNSIGNED NOT NULL AUTO_INCREMENT,"
        "pan_cnt BIGINT UNSIGNED NOT NULL,"
        "txn_cnt BIGINT UNSIGNED NOT NULL,"
        "txn_gbp_amt FLOAT NOT NULL,"
        "time_id INT NOT NULL,"
        "cardholder_id INT NOT NULL,"
        "merchant_id INT NOT NULL,"
        "cat1_id INT NOT NULL,"
        "cat2_id INT NOT NULL,"
        "cat3_id INT NOT NULL,"
        "PRIMARY KEY (id)"
        # " ENGINE=ColumnStore"
        ")")

    create_strings['fact2'] = ( 
        "CREATE TABLE fact2 ("
        "id INT UNSIGNED NOT NULL AUTO_INCREMENT,"
        "pan_cnt BIGINT UNSIGNED NOT NULL,"
        "txn_cnt BIGINT UNSIGNED NOT NULL,"
        "txn_gbp_amt FLOAT NOT NULL,"
        "merchant_outlet_count INT UNSIGNED NOT NULL,"
        "percent_repeat FLOAT NOT NULL,"
        "time_id INT NOT NULL,"
        "cardholder_id INT NOT NULL,"
        "category_id INT NOT NULL,"
        "m_channel_id INT NOT NULL,"
        "PRIMARY KEY (id)"
        # " ENGINE=ColumnStore"
        ")")

    create_strings['fact3'] = ( 
        "CREATE TABLE fact3 ("
        "id INT UNSIGNED NOT NULL AUTO_INCREMENT,"
        "perc_rail FLOAT NOT NULL,"
        "pan_cnt BIGINT UNSIGNED NOT NULL,"
        "txn_cnt BIGINT UNSIGNED NOT NULL,"
        "txn_gbp_amt FLOAT NOT NULL,"
        "time_id INT NOT NULL,"
        "merchant_id INT NOT NULL,"
        "jour_purpose_id INT NOT NULL,"
        "PRIMARY KEY (id)"
        # " ENGINE=ColumnStore"
        ")")

    create_strings['fact4'] = ( 
        "CREATE TABLE fact4 ("
        "id INT UNSIGNED NOT NULL AUTO_INCREMENT,"
        "perc_jour FLOAT NOT NULL,"
        "perc_pan FLOAT NOT NULL,"
        "time_id INT NOT NULL,"
        "merchant_id INT NOT NULL,"
        "transport_mode_id INT NOT NULL,"
        "PRIMARY KEY (id)"
        # " ENGINE=ColumnStore"
        ")")

    create_strings['time'] = ( 
        "CREATE TABLE time ("
        "raw VARCHAR(31) NOT NULL,"
        "year INT NOT NULL,"
        "quarter INT NOT NULL,"
        "month INT," # could also month_name varchar
        "id INT NOT NULL,"
        "PRIMARY KEY (id)"
        # " ENGINE=ColumnStore"
        ")")

    #     'location': ('location', 'sector', 'district', 'area', 'region', 'location_level', 'population', 'area_ha', 'id'),
    create_strings['location'] = ( 
        "CREATE TABLE location ("
        "location VARCHAR(63) NOT NULL,"
        "sector VARCHAR(6),"
        "district VARCHAR(4),"
        "area VARCHAR(2),"
        "region VARCHAR(63),"
        "location_level VARCHAR(31) NOT NULL,"
        "latitude FLOAT,"
        "longitude FLOAT,"
        "population INT,"
        "area_size INT,"
        "id INT NOT NULL,"
        "PRIMARY KEY (id)"
        # " ENGINE=ColumnStore"
        ")")

    create_strings['category'] = ( 
        "CREATE TABLE category ("
        "category VARCHAR(255) NOT NULL,"
        "id INT NOT NULL,"
        "PRIMARY KEY (id)"
        # " ENGINE=ColumnStore"
        ")")

    create_strings['m_channel'] = ( 
        "CREATE TABLE m_channel ("
        "m_channel VARCHAR(31) NOT NULL,"
        "id INT NOT NULL,"
        "PRIMARY KEY (id)"
        # " ENGINE=ColumnStore"
        ")")

    create_strings['purpose'] = ( 
        "CREATE TABLE purpose ("
        "purpose VARCHAR(63) NOT NULL,"
        "id INT NOT NULL,"
        "PRIMARY KEY (id)"
        # " ENGINE=ColumnStore"
        ")")

    create_strings['transport_mode'] = ( 
        "CREATE TABLE transport_mode ("
        "transport_mode VARCHAR(6) NOT NULL,"
        "id INT NOT NULL,"
        "PRIMARY KEY (id)"
        # " ENGINE=ColumnStore"
        ")")

    create_strings['census'] = ( 
        "CREATE TABLE census ("
        "location VARCHAR(6) NOT NULL," # maybe longer later if needed total scotland
        "population INT NOT NULL,"
        "id INT NOT NULL,"
        "PRIMARY KEY (id)"
        # " ENGINE=ColumnStore"
        ")")





elif (rdbms == 'postgresql'):

    create_strings['fact1'] = ( 
        "CREATE TABLE fact1 ("
        "id BIGSERIAL PRIMARY KEY,"
        "pan_cnt BIGINT NOT NULL,"
        "txn_cnt BIGINT NOT NULL,"
        "txn_gbp_amt FLOAT NOT NULL,"
        "time_id INT NOT NULL,"
        "cardholder_id INT NOT NULL,"
        "merchant_id INT NOT NULL,"
        "cat1_id INT NOT NULL,"
        "cat2_id INT NOT NULL,"
        "cat3_id INT NOT NULL"
        # "PRIMARY KEY (id)"
        # " ENGINE=ColumnStore"
        ")")

    create_strings['fact2'] = ( 
        "CREATE TABLE fact2 ("
        "id BIGSERIAL PRIMARY KEY,"
        "pan_cnt BIGINT NOT NULL,"
        "txn_cnt BIGINT NOT NULL,"
        "txn_gbp_amt FLOAT NOT NULL,"
        "merchant_outlet_count INT NOT NULL,"
        "percent_repeat FLOAT NOT NULL,"
        "time_id INT NOT NULL,"
        "cardholder_id INT NOT NULL,"
        "category_id INT NOT NULL,"
        "m_channel_id INT NOT NULL"
        # "PRIMARY KEY (id)"
        # " ENGINE=ColumnStore"
        ")")

    create_strings['fact3'] = ( 
        "CREATE TABLE fact3 ("
        "id BIGSERIAL PRIMARY KEY,"
        "perc_rail FLOAT NOT NULL,"
        "pan_cnt BIGINT NOT NULL,"
        "txn_cnt BIGINT NOT NULL,"
        "txn_gbp_amt FLOAT NOT NULL,"
        "time_id INT NOT NULL,"
        "merchant_id INT NOT NULL,"
        "jour_purpose_id INT NOT NULL"
        # "PRIMARY KEY (id)"
        # " ENGINE=ColumnStore"
        ")")

    create_strings['fact4'] = ( 
        "CREATE TABLE fact4 ("
        "id BIGSERIAL PRIMARY KEY,"
        "perc_jour FLOAT NOT NULL,"
        "perc_pan FLOAT NOT NULL,"
        "time_id INT NOT NULL,"
        "merchant_id INT NOT NULL,"
        "transport_mode_id INT NOT NULL"
        # "PRIMARY KEY (id)"
        # " ENGINE=ColumnStore"
        ")")

    create_strings['time'] = ( 
        "CREATE TABLE time ("
        "raw VARCHAR(31) NOT NULL,"
        "year INT NOT NULL,"
        "quarter INT NOT NULL,"
        "month INT," # could also month_name varchar
        "id INT NOT NULL,"
        "PRIMARY KEY (id)"
        # " ENGINE=ColumnStore"
        ")")

    #     'location': ('location', 'sector', 'district', 'area', 'region', 'location_level', 'population', 'area_ha', 'id'),
    create_strings['location'] = ( 
        "CREATE TABLE location ("
        "location VARCHAR(63) NOT NULL,"
        "sector VARCHAR(6),"
        "district VARCHAR(4),"
        "area VARCHAR(2),"
        "region VARCHAR(63),"
        "location_level VARCHAR(31) NOT NULL,"
        "latitude FLOAT,"
        "longitude FLOAT,"
        "population INT,"
        "area_size INT,"
        "density FLOAT,"
        "id INT NOT NULL,"
        "PRIMARY KEY (id)"
        # " ENGINE=ColumnStore"
        ")")

    create_strings['category'] = ( 
        "CREATE TABLE category ("
        "category VARCHAR(255) NOT NULL,"
        "id INT NOT NULL,"
        "PRIMARY KEY (id)"
        # " ENGINE=ColumnStore"
        ")")

    create_strings['m_channel'] = ( 
        "CREATE TABLE m_channel ("
        "m_channel VARCHAR(31) NOT NULL,"
        "id INT NOT NULL,"
        "PRIMARY KEY (id)"
        # " ENGINE=ColumnStore"
        ")")

    create_strings['purpose'] = ( 
        "CREATE TABLE purpose ("
        "purpose VARCHAR(63) NOT NULL,"
        "id INT NOT NULL,"
        "PRIMARY KEY (id)"
        # " ENGINE=ColumnStore"
        ")")

    create_strings['transport_mode'] = ( 
        "CREATE TABLE transport_mode ("
        "transport_mode VARCHAR(6) NOT NULL,"
        "id INT NOT NULL,"
        "PRIMARY KEY (id)"
        # " ENGINE=ColumnStore"
        ")")

    create_strings['census'] = ( 
        "CREATE TABLE census ("
        "location VARCHAR(6) NOT NULL," # maybe longer later if needed total scotland
        "population INT NOT NULL,"
        "id INT NOT NULL,"
        "PRIMARY KEY (id)"
        # " ENGINE=ColumnStore"
        ")")

else:
    raise Exception('something wrong with the code, unrecognised rdbms.')

