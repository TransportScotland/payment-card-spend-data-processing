# import pandas as pd
# import sqlalchemy

# con = sqlalchemy.create_engine('mysql://temp_user:password@localhost:3306/sgov')
# df = pd.read_sql('select * from census;', con)

# # df = df.loc[~ df['location'].str.contains(' ')]
# # df = df.loc[df['location'].str.contains('\d')]
# # print(df)
# # print(len(df))
# # 435 distinct districts. 435^2 = 189,225 district-to-district travel distances (950 GBP on gmaps)

# # sectors = df.loc[df['location'].str.contains(' ')]['location']
# # with open('out/all_sectors.txt', 'w') as file:
# #     for sector in list(sectors):
# #         file.write(sector+ '\n')

# # dic = {}
# # ls = []
# # with open('other_data/scotland_sectors_geocode.csv') as infile:
# #     _ = next(infile)
# #     import csv
# #     csvreader = csv.reader(infile)
# #     for line in csvreader:
# #         # swap rows 1 and 2 and ignore 0
# #         # dic[line[0]] = ([line[2], [line[1]]])
# #         ls.append((line[2], line[1]))
# # print(f'len(ls): {len(ls)}')

# # with open('out/scotland_sector_geo_lng_lat.json', 'w') as outfile:
# #     import json
# #     dic = {"locations": ls}
# #     s = json.dumps(dic)
# #     outfile.write(s)

# js = {}
# with open('other_data/ors_service_response.json') as file:
#     file_str = file.read()
#     import json
#     js = json.loads(file_str)
#     # print(js)

#     # print('js keys')
#     # print(js.keys())
#     # print(js['durations'][1][:10])
#     # # print(js['destinations'][:10])
#     # # print(js['sources'][:10])
#     # # print(js['metadata'])
#     # print(js['metadata'].keys())
#     for key in js['metadata'].keys():
#         if key != 'query':
#             print(f"{key}: {js['metadata'][key]}")


#     # print(len(js['durations']))
#     # print(len(js['destinations']))
#     # print(len(js['sources']))
#     # print(len(js['metadata']))


# sectors = df.loc[df['location'].str.contains(' ')]['location']
# sectors_list = list(sectors)
# with open('out/durations_matrix.csv', 'w') as file:
#     file.write('"durations (in seconds. down rows are origins, right columns are destinations. source: Open Route Service)",' + ','.join(sectors_list) + '\n')
#     # probably meters but tbc
#     assert(len(js['durations']) == len(sectors_list))

#     for i in range(len(sectors_list)):
#         row = js['durations'][i]
#         # print(len(row))
#         # for column in row:
#         file.write(sectors_list[i] + ',' )
#         file.write(','.join([str(e) for e in row]))
#         file.write('\n')









#### 
# Some funky stuff going on with the postcodes.
#
# There are large-user postcodes intended for routing mail to large corporate
# buildings but they don't seem to actually have a real location.
# 
# There are terminated postcodes which are no longer in use.
#
# And then there are live small-user postcodes which are special in some way
# or another (e.g. election counting shenanigans), 
# but should not affect me because they also don't really belong to any area.
####


# import pandas as pd

# df = pd.read_csv('other_data/postcode_census_zones.csv')
# # print('read csv')
# undeleted = df.loc[df['DateOfDeletion'].isnull()]
# uniq = undeleted['PostcodeSector'].unique()
# # uniq = df['PostcodeSector'].unique()
# # print(uniq)
# all_sectors = uniq
# # s_pcz = set(uniq)


# df2 = pd.read_csv('other_data/scotland_sectors_geocode.csv')
# located_sectors = df2['Address']
# # s_ssg = set(located_sectors)

# df3 = pd.read_csv('../other_data/open_postcode_geo_scotland.csv', header=None, usecols=[1,2,14])
# print(df3)
# df3_smallonly = df3.loc[df3[2] != 'large']
# df3_livesmall = df3_smallonly.loc[df3[1] == 'live']
# # df3_smallonly = df3.loc[df3[2] == 'small']
# print(df3_livesmall)
# uniq3 = df3_livesmall[14].unique()
# # s_opgs = set(uniq3)

# # sd_12 = s_pcz.symmetric_difference(s_ssg)
# # sd_13 = s_pcz.symmetric_difference(s_opgs)
# # sd_23 = s_ssg.symmetric_difference(s_opgs)
# # print(sd_12)
# # print(sd_13)
# # print(sd_23)


# # # difference between pcs and ssg seems to be 'large user' postcodes - corporate delivery facilities and such
# # temp = pd.merge(pd.Series(uniq, name='a'), pd.Series(located_sectors, name='b'), 
# #     'outer', left_on = 'a', right_on='b' )
# # print(temp.loc[temp['a'].isna() | temp['b'].isna()])

# temp = pd.merge(pd.Series(uniq3, name='a'), pd.Series(located_sectors, name='b'), 
#     'outer', left_on = 'a', right_on='b' )
# print(temp.loc[temp['a'].isna() | temp['b'].isna()])


# print(f'len all 1: {len(all_sectors)}, len geoloc: {len(located_sectors)}, len all 2 {len(uniq3)}')





# with open('data/file1_pa_1e4.csv') as file:
#     i = 0
#     for line in file:
#         split = line.split(',')
#         if (len(split) >= 5 and split[4] == 'POSTCODE_DISTRICT'):
#             print(line)
#             i +=1
#             if (i > 20):
#                 break