

# Code snippets
## SQL add user
(this is the most importand note here)

    create user 'temp_user'@'localhost' identified by 'password';
    create databse sgov;
    grant create, drop, select, insert, execute, alter, index, update, delete on sgov.* to 'temp_user'@'localhost';

Power BI is a bit annoying with same tables with different links, need to duplicate tables (click New Table; newT = 'originalT')

## Pandas display more columns
    pd.set_option('display.max_columns', 20)


# notes on speed
MySQL built-in load csv takes 10s on the raw dataset or 5.2s on the csv (with 1 mil rows), when no id provided (6s with ID). So it won't get any faster than that (same performance with both InnoDB and MyISAM.)
So I guess my current limit is ~13s with the intermediate csv step - 7s for process, 6s for load.
And my current speed with batch size 10,000 and unproteced querys is 16s. same w batch size 1000. 19 seconds for size 100, 88 seconds for size 1. Seems faster with powers of 10^k than 2*10^k (eg 1000 faster than 2000).
25s with a parameterised query batch 1000.
20s with escaping and encoding strings, bacth 1000.
17s with a type int float assert

See https://www.slideshare.net/billkarwin/load-data-fast (esp slide 50) for a speed comparison 

Time inserting (with transformations) of 1 million rows:
* SQL (experimental/file1_sql.py):     42 seconds
* Dask (experimental/file1_dask.py):   55 seconds (38 of that is uploading fact table to_sql)
* my row-wise solution (etl/file1.py): 18 seconds

* 4 batches of 1k ended up slower than 1 batch of 4k for each thread