exit() 
# ^ adding this here to prevent accidentally running this whole file as a script.
# it's meant to be more of a collection of code that was used to generate 
# a travel duration matrix, which should not need to be run again.
# If it is, I may change it to be more like a regular script.
import pandas as pd
import sqlalchemy

# connect to existing database with postcode sectors to query
con = sqlalchemy.create_engine('mysql://temp_user:password@localhost:3306/sgov')
df = pd.read_sql('select * from census;', con)

# isolate sectors and write to a text file
sectors = df.loc[df['location'].str.contains(' ')]['location']
with open('out/all_sectors.txt', 'w') as file:
    for sector in list(sectors):
        file.write(sector+ '\n')

##### 
# geocode sectors somehow,
# currently being done with a third-party app,
# could also be done by averaging over postcode unit locations from open data by the gov
#####


# convert geocoded sectors from lat-lng to lng-lat for OpenRouteService
dic = {}
ls = []
with open('other_data/scotland_sectors_geocode.csv') as infile:
    _ = next(infile)
    import csv
    csvreader = csv.reader(infile)
    for line in csvreader:
        # swap rows 1 and 2 and ignore 0
        # dic[line[0]] = ([line[2], [line[1]]])
        ls.append((line[2], line[1]))
print(f'len(ls): {len(ls)}')

# save the lng-lat locations, now without corresponding sector name
with open('out/scotland_sector_geo_lng_lat.json', 'w') as outfile:
    import json
    dic = {"locations": ls}
    s = json.dumps(dic)
    outfile.write(s)


#####
# now send the coordinates to OpenRouteService matrix service.
# this could be done in Python easily if needed,
# here I just sent a post request manually using Postman and saved the response
#####



# load the response into a python dict
js = {}
with open('other_data/ors_service_response.json') as file:
    file_str = file.read()
    import json
    js = json.loads(file_str)
    # print(js.keys())
    # # ['durations', 'destinations', 'sources', 'metadata']
    for key in js['metadata'].keys():
        # skipping query metadata because it's too long to display
        if key != 'query': 
            print(f"{key}: {js['metadata'][key]}")


# save the response in a csv format
sectors = df.loc[df['location'].str.contains(' ')]['location']
sectors_list = list(sectors)
with open('out/durations_matrix.csv', 'w') as file:
    file.write('"durations (in seconds. down rows are origins, right columns are destinations. source: Open Route Service)",' + 
        ','.join(sectors_list) + '\n')

    assert(len(js['durations']) == len(sectors_list))

    for i in range(len(sectors_list)):
        row = js['durations'][i]
        file.write(sectors_list[i] + ',' )
        file.write(','.join([str(e) for e in row]))
        file.write('\n')

