
# Execution flow overview

Execution starts in file /etl/run.py, which just calls all the other modules in order. First, CSV versions of census/location/distance data are converted to a json dict which can then be quickly accessed when loading the card data modules. Then, some or all of the fileN.py main functions are called.

Taking module1 as an example, the zip file is processed as a stream and it is unzipped as the lines of the csv are being processed, all lines being discarded after a batch is processed because the whole files would not fit into memory.  

The module file1 defines a function which will be applied to all rows of the input csv, it takes in a list of cells in the original csv and returns an updated version with more cells added at the end. The columns of the newly created table are also defined in the file1 module, as well as the zip passwords and file paths. The etl() function just calls the apply_and_save function in shared_funcs, passing in the name of the row-modifying function.

The apply_and_save function does what it sounds like, it reads the input zip file, applied the passed in row_function on each row, and saves each row into the database. This is currently done by saving to a csv and then using the official command-line database client to import the CSV into the database.


# Code snippets
## Pandas display more columns
* `pd.set_option('display.max_columns', 20)`

## To change directory of a new Clickhouse installation

0. stop clickhouse server service: `sudo service clickhouse-server stop`
1. change some or all mentions of `/var/lib/clickhouse` to `/my_directory/clickhouse` in the file `/etc/clickhouse-server/config.xml`
2. Try restarting the service with `sudo service slickhouse-server start`. This has a high chance of not working just by itself (esp on mounted disks), so 
3. Change the permissions of your new data folder. Clickhouse needs to read, write, and execute in this folder, so, assuming the folder already exists:
    `sudo chmod o+rwx /my_directory/clickhouse`



* It may be really worth creating (materialzed) views of the whole database with quarterly vs monthly views, district vs sector area region views etc. bc currently monthly and quarterly data seems to be mixed together so some people are being counted twice and I need to filter by month being null

# A note about BT1 5 postcode sector
This one shows up in the top spots often when dividing by population. This is because in the 2011 census data, the BT1 5AB postcode was the only postcode in the data and it even had less than 10 inhabitants. Either the census data was wrong, or there are now more postcodes under the BT1 5 postcode sector, possibly re-assigned from other postcodes. So the anomaly is caused by census data, not our payment card data.

# notes on database choice

There is a lot of data, and Power BI downloads data into the local pbix file by default, which won't work with how much data there is. Luckily, it also supports querying the database directly using the Direct Query option instead of Import. Unfortunately, This only works with a very limited set of data sources (with MySQL missing). Furthermore, most OLTP RDBMSs store data as rows, which is much much slower for analysis tasks than storing it by columns. This leads to a few options for storing our data:

* Regular MySQL or PostgreSQL were tried and would be too slow (more than 10 minutes for a basic query, BI tools time out). MS SQL Server also doesn't seem any better.
* MariaDB with ColumnStore engine (open source fork of MySQL by its original creators), PostgreSQL with Citus extension (with column store option), Apache Spark SQL Thrift server with Parquet files or Hive, were all tried and performed similarly (Spark also took two days to figure out how to run).
* Clickhouse is currently being used because it performed much faster (roughly 5x)
* Other local options exist - most notably MonetDB which claims to be even faster (especially with joins), but it is less commonly used and there don't seem to be any plugins to make it work with Metabase, so Apache Superset or Tableau would have to be used instead.
* Google BigQuery or Amazon RedShift (or Azure's version, or Apache Druid). Made for real-time queries on much bigger data sets so will be the fastest, but they cost money - although it is a small amount of money - BigQuery is $5 per 1TB of queries, with the first 1TB every month being free (1TB is roughly 50 queries on the current dataset of 1 year). Storage is $0.02 per GB with first 10GB per month free (so $0.2 for our 20GB data, or $0.4 without the free 10GB). But using it with Power BI may produce more queries than expected at first which may end up costing a lot. But even then, it should be really cheap for our case. Not currently used.


# Other options for data visualisations

* currently using Metabase (not owned by Facebook/Meta despite the name)
* Apache Superset - more popular open source BI tool. To install check the dockerhub page (including the part about adding plugins). Adding maps requires re-compiling with Node.js which seems to take hours if it completes at all.
* Power BI on a separate Windows machine if the data is secure enough. (or it may run in Wine on Linux).
* Tableau - again doesn't work on Linux, and free version *may* steal data.


# maps
For adding maps to metabase, see https://www.metabase.com/docs/latest/administration-guide/20-custom-maps.html.
Currently being used are maps created by GeoLytix, full attribution in `/attrbutions.txt` file.
Also consider UK postcode district shape maps https://github.com/missinglink/uk-postcode-polygons.