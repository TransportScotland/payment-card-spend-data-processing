
# Execution flow overview

The input files are processed in a row-wise manner as opposed to a column-wise manner as may be sometimes common with other ETL processes. This is done for performance reasons, as it was found to be more than 2x faster than performing the ETL process even directly in MySQL. Because of this, the code may be more complicated and harder to read, so an overview of the execution flow is provided below. Though, if a column-wise solution is desired instead, a just about finished one for the first file is available in /experimental/file1_dask_map.py and /experimental/file1_sql.py.

Execution starts in file /etl/run.py, which just calls all the other modules in order. Loading of the location+census and distance data is done separately using pandas and should be mostly self-explanatory.

The payment card data files are processed one by one, though the dimension tables are stored in-memory until after all of the input files have been processed. The fact table rows are saved in batches of up to 4000, which drastically improves speed over sending Insert requests one at a time. Saving into the database is a big bottleneck, so it is done in parallel - the insert request is sent and the next batch of input rows keeps being processed while the database is busy.

Before anything is saved in the fact tables or any other tables, the old data is dropped (though this behaviour can be changed).

As the input data is processed, the in-memory dimension tables are filled. These are esentially python dicts (Dictionaries) with some extra functionality added to make it possible and easier to save as tables into SQL as dimension tables (namely ensuring the indices stay consistent). 
Every dimension table insertion returns an ID, which is then used in much the same fashion as a Foreign Key in the fact table. Once all dimensions in one row are processed and replaced with IDs, the row is added to a queue, and when that queue is filled to 4000, it is sent to MySQL in a separate thread.

The main functions to look at when trying to understand this code are fileX.etl(), batch_process_threaded_from_generator() (pending rename), and fileX.create_dims (also pending rename)


# Code snippets
## SQL add user
(this is the most importand note here)

    create user 'temp_user'@'localhost' identified by 'password';
    create database sgov;
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


# maps
For adding maps to metabase, see https://www.metabase.com/docs/latest/administration-guide/20-custom-maps.html. Also consider UK postcode district shape maps https://github.com/missinglink/uk-postcode-polygons. Sector maps are not readily available but can be created with open source code.