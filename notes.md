
# Execution flow overview

The input files are processed in a row-wise manner as opposed to a column-wise manner as may be sometimes common with other ETL processes. This is done for performance reasons, as it was found to be more than 2x faster than performing the ETL process even directly in MySQL. Because of this, the code may be more complicated and harder to read, so an overview of the execution flow is provided below. Though, if a column-wise solution is desired instead, a just about finished one for the first file is available in /experimental/file1_dask_map.py and /experimental/file1_sql.py.

Execution starts in file /etl/run.py, which just calls all the other modules in order. Loading of the location+census and distance data is done separately using pandas and should be mostly self-explanatory.

The payment card data files are processed one by one, though the dimension tables are stored in-memory until after all of the input files have been processed. The fact table rows are saved in batches of up to 4000, which drastically improves speed over sending Insert requests one at a time. Saving into the database is a big bottleneck, so it is done in parallel - the insert request is sent and the next batch of input rows keeps being processed while the database is busy.

Before anything is saved in the fact tables or any other tables, the old data is dropped (though this behaviour can be changed).

As the input data is processed, the in-memory dimension tables are filled. These are esentially python dicts (Dictionaries) with some extra functionality added to make it possible and easier to save as tables into SQL as dimension tables (namely ensuring the indices stay consistent). 
Every dimension table insertion returns an ID, which is then used in much the same fashion as a Foreign Key in the fact table. Once all dimensions in one row are processed and replaced with IDs, the row is added to a queue, and when that queue is filled to 4000, it is sent to MySQL in a separate thread.

The main functions to look at when trying to understand this code are fileX.etl(), batch_process_threaded_from_generator() (pending rename), and fileX.create_dims (also pending rename)

# To change Clickhouse directory

0. stop clickhouse server service: `sudo service clickhouse-server stop`
1. change some or all mentions of `/var/lib/clickhouse` to `/my_directory/clickhouse` in the file `/etc/clickhouse-server/config.xml`
2. Try restarting the service with `sudo service slickhouse-server start`. This has a high chance of not working just by itself (esp on mounted disks), so 
3. Change the permissions of your new data folder. Clickhouse needs to read, write, and execute in this folder, so, assuming the folder already exists:
    `sudo chmod o+rwx /my_directory/clickhouse`



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


* It may be really worth creating (materialzed) views of the whole database with quarterly vs monthly views, district vs sector area region views etc. bc currently monthly and quarterly data seems to be mixed together so some people are being counted twice and I need to filter by month being null

# A note about BT1 5 postcode sector
This one shows up in the top spots often when dividing by population. This is because in the 2011 census data, the BT1 5AB postcode was the only postcode in the data and it even had less than 10 inhabitants. Either the census data was wrong, or there are now more postcodes under the BT1 5 postcode sector, possibly re-assigned from other postcodes. So the anomaly is caused by census data, not our payment card data.

# notes on database choice

There is a lot of data, and Power BI downloads data into the local pbix file by default, which won't work with how much data there is. Luckily, it also supports querying the database directly using the Direct Query option instead of Import. Unfortunately, This only works with a very limited set of data sources (with MySQL strangely missing). Furthermore, most OLTP RDBMSs store data as rows, which is much much slower for analysis tasks than storing it by columns. This leads to a few options for storing our data:

* MariaDB. An open source fork of MySQL by its original creators (after MySQL was acquired by Oracle). It supports a ColumnStore engine, but only on Linux. Which would be fine, but then Power BI says it supports Direct Query but when it comes to actually showing any visualisations, it is unable to create even the most basic ones, saying it was unable to fold the query. This may be a kink with my installation of Power BI so this is to be confirmed.
* PostgreSQL with Citus extension. (Citus gives a column store option). Should work but I do not want to risk it
* Apache Spark with Parquet files or Hive. Again don't want to risk changing everything for it to just not work. But may be the fastest open source option.
* Google BigQuery or Amazon RedShift. Made for real-time queries on much bigger data sets so will likely be the fastest, but they cost money - although it is a small amount of money - BigQuery is $5 per 1TB of queries, with the first 1TB every month being free (1TB is roughly 50 queries on the current dataset of 1 year). Storage is $0.02 per GB with first 10GB per month free (so $0.2 for our 20GB data, or $0.4 without the free 10GB). But using it with Power BI may produce more queries than expected at first which may end up costing a lot.
* Microsoft SQL Server. Column stores unclear how good, will need to create a columnar index on every column. This is the most likely to work, but potentially the most expensive. Developer edition is free but not allowed to be used in production - whether that applies to our case is a mystery.
* Another option is to use Tableau or Talend with self-hosted MariaDB or other db in the hope that they will work better together.
    * If using Tableau or Talend, there may be more options yet, like Clickhouse


# Other options for data visualisations

* Power BI
* Tableau 
* Apache Superset
    * FOSS
    * may need to know SQL?
* Dash (Plotly)
    * looks really cool for data vis but deals with pandas dataframes which won't fit in memory for us
* Metabase (not owned by Facebook/Meta despite the name)


# maps
For adding maps to metabase, see https://www.metabase.com/docs/latest/administration-guide/20-custom-maps.html. Also consider UK postcode district shape maps https://github.com/missinglink/uk-postcode-polygons. Sector maps are not readily available but can be created with open source code.