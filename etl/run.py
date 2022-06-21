
import time
import shared_funcs
import file1, file2, file3, file4

data_folder = 'data/'
facts_1 = file1.etl('data/file1.csv', 'wh/fact1.csv')
facts_2 = file2.etl('data/file2.csv', 'wh/fact2.csv')
facts_3 = file3.etl('data/file3.csv', 'wh/fact3.csv')
facts_4 = file4.etl('data/file4.csv', 'wh/fact4.csv')

# data_folder = '../sample_data/'
# file_names = ['Network_Rail_File1_Spend Origin.csv',
# 'Network_Rail_File2_Origin Spend by Channel.csv',
# 'Network_Rail_File3_Journey Purpose.csv',
# 'Network_Rail_File4_Modal Shift.csv',]
# facts_1 = file1.etl(data_folder, file_names[0])
# facts_2 = file2.etl(data_folder, file_names[1])
# facts_3 = file3.etl(data_folder, file_names[2])
# facts_4 = file4.etl(data_folder, file_names[3])

if shared_funcs.skipped_rows:
    print()
    print('skipped rows:')
    for r in shared_funcs.skipped_rows:
        print(r)


wh_folder = 'wh/'
# time0 = time.time()
# shared_funcs.list_to_csv(facts_1, file1.fact_headers, wh_folder, 'fact1.csv' )
# time1 = time.time()
# print(f'write took {time1-time0} seconds')
# shared_funcs.list_to_csv(facts_2, file2.fact_headers, wh_folder, 'fact2.csv' )
# shared_funcs.list_to_csv(facts_3, file3.fact_headers, wh_folder, 'fact3.csv' )
# shared_funcs.list_to_csv(facts_4, file4.fact_headers, wh_folder, 'fact4.csv' )
shared_funcs.dims_to_csv(wh_folder)
