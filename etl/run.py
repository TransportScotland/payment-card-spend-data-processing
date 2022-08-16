#!/usr/bin/env python3
#line above to prevent accidentally executing as a bash script on a linux machine when the file is on an NTFS-formatted disk partition

#import python libraries 
import time

#import my python modules
import shared_funcs
import file1, file2, file3, file4
import distances, census_data

# start a timer
time0 = time.time()

# todo set up the database connection here

census_data.etl_default_files()

# call each of the files' relevant ETL function to read the file, transform it, and load into the database
# first deal with the main data sets of card data

# f1p = 'temp_head100.csv'
# f2p = 'temp2_head100.csv'
f1p = 'C:/SFTP/san-ssapfs/edge/home/chaudhup/Network_Rail/nr_module_1_2021.csv'
f2p = 'C:/SFTP/san-ssapfs/edge/home/chaudhup/Network_Rail/nr_module_2_2021.csv'
file1.etl(f1p) # smaller file for testing purposes
file2.etl(f2p)
# file3.etl('data/file3.csv')
# file4.etl('data/file4.csv')

# save the dimensions into the database. This needs to be called, otherwise the database will have only the fact tables
shared_funcs.save_dims() # TODO only save after districts fixed, and make that fn use in-mem table

print(f'skipped {len(shared_funcs.district_rows)} rows to be de-aggregated later')
# subtract sums of sector values from district values
file1.fix_districts()

shared_funcs.save_dim('location') 


# combine with other datasets
# TODO add --reload-distances flag (or other datasets) to not load in distances every time (default False, maybe also do a check if exists in db)
# census_data.etl_old('other_data/KS101SC.csv')
distances.etl(['generated_data/durations_matrix.csv']) # needs to be run after census
# distances.etl(['generated_data/durations_matrix_20rows.csv']) # small subset for testing purposes



# stop timer and print total time taken
time1 = time.time()
print(f'The extract, transform, and load process took {time1-time0} seconds.')

# # print error rows. currently unused, once implemented probably output to a file instead
# if shared_funcs.skipped_rows:
#     print()
#     print('skipped rows:')
#     for r in shared_funcs.skipped_rows:
#         print(r)
