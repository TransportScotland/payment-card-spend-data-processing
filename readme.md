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

An overview of the code execution flow is available in /notes.md.
