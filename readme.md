# Payment card data processing

This is the code used for extracting, transforming, loading, and analysing payment card data purchased by Network Rail and shared with Transport Scotland.

# Getting started

Assuming you have the data in the same format, run the Extract, Transform, and Load (ETL) process by running the following command from the root directory in the command line.

    python3 etl/run.py

You may need to change the file paths of the input files as appropriate.

This also requires having other datasets downloaded:

* Scottish census data by postcode sector in other_data/KS101SC.csv
* postcode location data in other_data/open_postcode_geo_scotland.csv
* distance matrix (already in this repository) in generated_data/durations_matrix.csv

# Code execution flow overview.

The input files are processed in a row-wise manner as opposed to a column-wise manner as may be sometimes common with other ETL processes. This is done for performance reasons, as it was found to be more than 2x faster than performing the ETL process even directly in MySQL. Because of this, the code may be more complicated and harder to read, so an overview of the execution flow is provided below. Though, if a column-wise solution is desired instead, a just about finished one for the first file is available in /experimental/file1_dask_map.py and /experimental/file1_sql.py.

Execution starts in file /etl/run.py, which just calls all the other modules in order. Loading of the location+census and distance data is done separately using pandas and should be mostly self-explanatory.

The payment card data files are processed one by one, though the dimension tables are stored in-memory until after all of the input files have been processed. The fact table rows are saved in batches of up to 4000, which drastically improves speed over sending Insert requests one at a time. Saving into the database is a big bottleneck, so it is done in parallel - the insert request is sent and the next batch of input rows keeps being processed while the database is busy.

Before anything is saved in the fact tables or any other tables, the old data is dropped (though this behaviour can be changed).

As the input data is processed, the in-memory dimension tables are filled. These are esentially python dicts (Dictionaries) with some extra functionality added to make it possible and easier to save as tables into SQL as dimension tables (namely ensuring the indices stay consistent). 
Every dimension table insertion returns an ID, which is then used in much the same fashion as a Foreign Key in the fact table. Once all dimensions in one row are processed and replaced with IDs, the row is added to a queue, and when that queue is filled to 4000, it is sent to MySQL in a separate thread.

The main functions to look at when trying to understand this code are fileX.etl(), batch_process_threaded_from_generator() (pending rename), and fileX.create_dims (also pending rename)
