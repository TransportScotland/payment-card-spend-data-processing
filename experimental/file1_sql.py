from typing import Iterable
import dask.dataframe as dd
import pandas as pd
import sqlalchemy

pd.set_option('display.max_columns', 20)


dflib = dd


def read_raw(file, dbcon):
    dbcon.execute(sqlalchemy.text(
        ( 
        "DROP TABLE IF EXISTS fact1_raw;"
        "CREATE TABLE fact1_raw ("
        "time_frame VARCHAR(31) NOT NULL,"
        "cardholder_type VARCHAR(31) NOT NULL,"
        "cardholder_location_level VARCHAR(63) NOT NULL,"
        "cardholder_location VARCHAR(31) NOT NULL,"
        "merchant_location_level VARCHAR(63) NOT NULL,"
        "merchant_location VARCHAR(31) NOT NULL,"
        "pan_cnt INT NOT NULL,"
        "txn_cnt INT NOT NULL,"
        "txn_gbp_amt FLOAT NOT NULL,"
        "mcc_rank1 VARCHAR(63) NOT NULL,"
        "mcc_rank2 VARCHAR(63) NOT NULL,"
        "mcc_rank3 VARCHAR(63) NOT NULL,"
        "id INT NOT NULL AUTO_INCREMENT,"
        "PRIMARY KEY (id)"
        ");")
        ))

            # '/media/fpavlica/Data/school/work/2022_summer_scotgov/code/pre_work/data/fact1_2k.csv' 
    dbcon.execute(sqlalchemy.text(f"""LOAD DATA LOCAL INFILE 
        '{file}'
        INTO TABLE fact1_raw
        FIELDS TERMINATED BY ',' 
        ENCLOSED BY '"' 
        LINES TERMINATED BY '\n' 
        IGNORE 1 ROWS 
        (time_frame,cardholder_type,cardholder_location_level,cardholder_location,merchant_location_level,merchant_location,
            pan_cnt,txn_cnt,txn_gbp_amt,mcc_rank1,mcc_rank2,mcc_rank3);"""))


def create_time_table(dbcon):
    # create new table from time column
    dbcon.execute("drop table if exists time;")
    dbcon.execute("create table time as (select distinct time_frame as time_raw from fact1_raw);")
    dbcon.execute("alter table time add time_id int primary key auto_increment;")
    dbcon.execute("alter table time add year varchar(4);")
    dbcon.execute("update time set year=substring(time_raw, 1, 4);")
    dbcon.execute("alter table time add month varchar(2);")
    dbcon.execute("update time set month=substring(time_raw, 5, 2);")

    dbcon.commit()

def create_location_table(dbcon):
    dbcon.execute("drop table if exists location;")
    dbcon.execute("""create table location as (
        select distinct(cardholder_location) as location_raw from fact1_raw 
        union
        select distinct(merchant_location) as location_raw from fact1_raw
        );""")
    dbcon.execute("alter table location add location_id int primary key auto_increment;")
    dbcon.execute("alter table location add sector varchar(6);")
    dbcon.execute("alter table location add district varchar(4);")
    dbcon.execute("alter table location add area varchar(2);")
    dbcon.execute("update location set sector=location_raw;")
    dbcon.execute("update location set district=substring(sector, 1, char_length(sector) -2);")
    dbcon.execute("update location set area= regexp_substr(sector, '^[A-Z]{1,2}');")
    dbcon.commit()
    
    pass

def create_simple_dim_table(dbcon, name, original_cols):
    if type(original_cols) == str:
        original_cols = [original_cols] 
    dbcon.execute(f"drop table if exists {name};")
    table_select = '\nunion\n'.join([f"select distinct({col}) as {name} from fact1_raw" for col in original_cols])
    dbcon.execute(f"create table {name} as ({table_select});")
    dbcon.execute(f"alter table {name} add {name}_id int primary key auto_increment;")
    dbcon.commit()


def merges(dbcon):
    dbcon.execute("drop table if exists fact1;")
    dbcon.execute("""create table fact1 as (
        select pan_cnt, txn_cnt, txn_gbp_amt, time_id, cardholder_location_id, merchant_location_id,
            mcc_rank1_id, mcc_rank2_id, mcc_rank3_id, cardholder_type_id
        from 
            fact1_raw f 
                inner join (select time_raw, time_id from time) t on f.time_frame = t.time_raw 
                inner join (select location_raw as lr1, location_id as cardholder_location_id from location) l1 on f.cardholder_location = l1.lr1
                inner join (select location_raw as lr2, location_id as merchant_location_id from location) l2 on f.merchant_location = l2.lr2
                inner join (select category as cf1, category_id as mcc_rank1_id from category) ct1 on ct1.cf1=f.mcc_rank1
                inner join (select category as cf2, category_id as mcc_rank2_id from category) ct2 on ct2.cf2=f.mcc_rank2
                inner join (select category as cf3, category_id as mcc_rank3_id from category) ct3 on ct3.cf3=f.mcc_rank3
                inner join cardholder_type cht on f.cardholder_type = cht.cardholder_type
        );""")
    dbcon.execute("alter table fact1 add id int not null primary key auto_increment;")

# def merges(dbcon):
#     # this time with updates instead of full joins
#     dbcon.execute("drop table if exists fact1_i;")
#     dbcon.execute("""create table fact1_i as (
#         select * from fact1_raw
#     );""")
#     dbcon.execute("""alter table fact1_i add column (
#         time_id int, 
#         cardholder_location_id int,
#         merchant_locaiton_id int,
#         mcc_rank1_id int,
#         mcc_rank2_id int,
#         mcc_rank3_id int,
#         chtype_id int
#         );""")
#     dbcon.execute("""update fact1_i 
#         set time_id= time.time_id from time where fact1_i.time_frame = time.time_raw;""")

#             # cardholder_location_id, merchant_location_id,
#             # mcc_rank1_id, mcc_rank2_id, mcc_rank3_id, cardholder_type_id
#     dbcon.execute("""create table fact1 as (
#         select pan_cnt, txn_cnt, txn_gbp_amt, time_id 
#         from 
#             fact1_i;""")
#     pass

def etl(file):

    sql_uri = 'mysql://temp_user:password@localhost/sgov?local_infile=1'
    dben = sqlalchemy.create_engine(sql_uri)
    from sqlalchemy.orm import sessionmaker
    dbcon = sessionmaker(bind=dben)()

    read_raw(file, dbcon)
    create_time_table(dbcon)
    create_location_table(dbcon)
    create_simple_dim_table(dbcon, 'category', ['mcc_rank1', 'mcc_rank2', 'mcc_rank3'])
    create_simple_dim_table(dbcon, 'cardholder_type', 'cardholder_type')
    merges(dbcon)
    
    # dbcon = dben.connect()
    # time_frame,cardholder_type,cardholder_location_level,cardholder_location,merchant_location_level,merchant_location,
    # pan_cnt,txn_cnt,txn_gbp_amt,mcc_rank1,mcc_rank2,mcc_rank3



    # dbcon.begin()
    # res= dbcon.execute("select * from fact1_raw limit 10;")
    # print(res.all())
    dbcon.close()

    



if __name__ == '__main__':
    import time
    time0 = time.time()

    etl('data/file1.csv')
    # etl('data/file1_2k.csv')

    time1 = time.time()
    print(f'Time taken: {time1-time0} seconds.')