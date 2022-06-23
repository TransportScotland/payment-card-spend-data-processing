

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
    'location': ('sector', 'district', 'area', 'region', 'id'),
    'category': ('category', 'id'),
    'm_channel': ('m_channel', 'id'),
    'purpose': ('purpose', 'id'),
    'transport_mode': ('transport_mode', 'id'),
    'census': ('location', 'population', 'id'),
}


create_strings = {}

create_strings['fact1'] = ( 
    "CREATE TABLE fact1 ("
    "id INT NOT NULL AUTO_INCREMENT,"
    "pan_cnt INT NOT NULL,"
    "txn_cnt INT NOT NULL,"
    "txn_gbp_amt FLOAT NOT NULL,"
    "time_id INT NOT NULL,"
    "cardholder_id INT NOT NULL,"
    "merchant_id INT NOT NULL,"
    "cat1_id INT NOT NULL,"
    "cat2_id INT NOT NULL,"
    "cat3_id INT NOT NULL,"
    "PRIMARY KEY (id)"
    ")")

create_strings['fact2'] = ( 
    "CREATE TABLE fact2 ("
    "id INT NOT NULL AUTO_INCREMENT,"
    "pan_cnt INT NOT NULL,"
    "txn_cnt INT NOT NULL,"
    "txn_gbp_amt FLOAT NOT NULL,"
    "merchant_outlet_count INT NOT NULL,"
    "percent_repeat FLOAT NOT NULL,"
    "time_id INT NOT NULL,"
    "cardholder_id INT NOT NULL,"
    "category_id INT NOT NULL,"
    "m_channel_id INT NOT NULL,"
    "PRIMARY KEY (id)"
    ")")

create_strings['fact3'] = ( 
    "CREATE TABLE fact3 ("
    "id INT NOT NULL AUTO_INCREMENT,"
    "perc_rail FLOAT NOT NULL,"
    "pan_cnt INT NOT NULL,"
    "txn_cnt INT NOT NULL,"
    "txn_gbp_amt FLOAT NOT NULL,"
    "time_id INT NOT NULL,"
    "merchant_id INT NOT NULL,"
    "jour_purpose_id INT NOT NULL,"
    "PRIMARY KEY (id)"
    ")")

create_strings['fact4'] = ( 
    "CREATE TABLE fact4 ("
    "id INT NOT NULL AUTO_INCREMENT,"
    "perc_jour FLOAT NOT NULL,"
    "perc_pan INT NOT NULL,"
    "time_id INT NOT NULL,"
    "merchant_id INT NOT NULL,"
    "transport_mode_id INT NOT NULL,"
    "PRIMARY KEY (id)"
    ")")

create_strings['time'] = ( 
    "CREATE TABLE time ("
    "raw VARCHAR(31) NOT NULL,"
    "year INT NOT NULL,"
    "quarter INT NOT NULL,"
    "month INT NOT NULL," # could also month_name varchar
    "id INT NOT NULL AUTO_INCREMENT,"
    "PRIMARY KEY (id)"
    ")")

create_strings['location'] = ( 
    "CREATE TABLE location ("
    "sector VARCHAR(6),"
    "district VARCHAR(4),"
    "area VARCHAR(2),"
    "region VARCHAR(63),"
    "id INT NOT NULL AUTO_INCREMENT,"
    "PRIMARY KEY (id)"
    ")")

create_strings['category'] = ( 
    "CREATE TABLE category ("
    "category VARCHAR(255) NOT NULL,"
    "id INT NOT NULL AUTO_INCREMENT,"
    "PRIMARY KEY (id)"
    ")")

create_strings['m_channel'] = ( 
    "CREATE TABLE m_channel ("
    "m_channel VARCHAR(31) NOT NULL,"
    "id INT NOT NULL AUTO_INCREMENT,"
    "PRIMARY KEY (id)"
    ")")

create_strings['purpose'] = ( 
    "CREATE TABLE purpose ("
    "purpose VARCHAR(63) NOT NULL,"
    "id INT NOT NULL AUTO_INCREMENT,"
    "PRIMARY KEY (id)"
    ")")

create_strings['transport_mode'] = ( 
    "CREATE TABLE transport_mode ("
    "transport_mode VARCHAR(6) NOT NULL,"
    "id INT NOT NULL AUTO_INCREMENT,"
    "PRIMARY KEY (id)"
    ")")

create_strings['census'] = ( 
    "CREATE TABLE census ("
    "location VARCHAR(6) NOT NULL," # maybe longer later if needed total scotland
    "population INT NOT NULL,"
    "id INT NOT NULL AUTO_INCREMENT,"
    "PRIMARY KEY (id)"
    ")")

