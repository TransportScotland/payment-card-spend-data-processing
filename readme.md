# Payment card data processing

This is the code used for extracting, transforming, and loading some (anonymised and aggregated) purchase card data.

# Getting started: Data analysis 

If your data is already loaded in a database, you can point any 'business intelligence' (BI) tool at it and start producing interactive dashboards.

The BI tool used here is Metabase, with a plugin to connect to the Clickhouse database. To start Metabase, go to the installation folder and run `java -jar metabase.jar`. If you want to also use shape file maps, navigate to a folder with them and run `python3 -m http.server` to serve them locally. You should now be able to access the Metabase UI by opening a browser and going to http://localhost:3000 (by default).

The clickhouse-server database should be running when the computer starts, but if it is not or you need to restart it, use `sudo service clickhouse-server stop` and `sudo service clickhouse-server start`.

# Getting started: Data loading

To load new files to the database, first check the file `etl/run.py` to see it is only set to load the modules you want it to. Loading a module if it is already loaded in will drop the table currently in the database and re-load it from scratch, which can take around 2 hours. (To track progress use `clickhouse-client --password` with a password and `select count(1) from db.table`). So it is better to only reload the modules that are not yet loaded in by checking the `etl/run.py` file does only what is needed.

The code is set up to work with zip files being in `/mnt/sftp/in`, if these are somewhere else then the file paths in the code may need to be changed.

Assuming everything works fine, it should then be enough to just call `python3 etl/run.py` from the root directory of this repository (i.e. where this readme file is and where the etl folder with the file run.py is).

Loading may also require having some other datasets downloaded:

* postcode location data in /other_data/open_postcode_geo_scotland.csv
* Scottish census data by postcode sector in /other_data/KS101SC.csv
* Northern Ireland, England, and Wales census data by postcode also in /other_data/
* distance data in /generated_data/out_card_trips_car.csv

If something goes wrong that is not very easy to fix, an overview of the code execution flow is available in notes.md.
