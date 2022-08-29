# Depracated

The files in this folder are no longer used, they are kept here for now just for keeping this repository complete but the folder can be removed soon.

It may also be useful if going back to a dimensional model (star schema) instead of a wide flat table.

The main difference between this /etl_legacy/ and the current /etl/ folder is that in here, the dimension tables are kept as separate python dictionaries until the files are loaded, and then added to the database as separate tables. Whereas in the more current implementation, the dimensions are stored with the fact table data in one table as rows as the input files are being processed.  