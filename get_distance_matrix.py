import pandas as pd
import sqlalchemy

# connect to existing database with postcode sectors to query
con = sqlalchemy.create_engine('mysql://temp_user:password@localhost:3306/sgov')
df = pd.read_sql('select * from census;', con)

# isolate sectors and write to a text file
sectors = df.loc[df['location'].str.contains(' ')]['location']
with open('generated_data/intermediate/all_sectors.txt', 'w') as file:
    for sector in list(sectors):
        file.write(sector+ '\n')

print('List of sectors saved to generated_data/intermediate/all_sectors.txt.')
print('Get the geo coordinates into generated_data/intermediate/scotland_sectors_geocode.csv ')
print('and press Enter to continue')
_ = input()
##### 
# geocode sectors somehow,
# currently being done with a third-party app,
# could also be done by averaging over postcode unit locations from open data by the gov
#####


# convert geocoded sectors from lat-lng to lng-lat for OpenRouteService
dic = {}
ls = []
with open('generated_data/intermediate/scotland_sectors_geocode.csv') as infile:
    _ = next(infile)
    import csv
    csvreader = csv.reader(infile)
    for line in csvreader:
        # swap rows 1 and 2 and ignore 0
        # dic[line[0]] = ([line[2], [line[1]]])
        ls.append((line[2], line[1]))
print(f'len(ls): {len(ls)}')

# save the lng-lat locations, now without corresponding sector name
with open('generated_data/intermediate/scotland_sector_geo_lng_lat.json', 'w') as outfile:
    import json
    dic = {"locations": ls}
    s = json.dumps(dic)
    outfile.write(s)



# now send the coordinates to OpenRouteService matrix service.
r= None
with open('generated_data/intermediate/scotland_sector_geo_lng_lat.json') as file:
    import json
    data = json.loads(file.read())
    # data['metrics'] = ['distance']

    import requests
    import time
    t1 = time.time()
    r = requests.post('http://localhost:8080/ors/v2/matrix/driving-car', json=data)
    t2 = time.time()
    print(f'ORS matrix request took {t2-t1} seconds.')

with open('generated_data/intermediate/ors_service_response.json', 'w') as outfile:
    outfile.write(r.text)


# load the response into a python dict
js = {}
with open('generated_data/intermediate/ors_service_response.json') as file:
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
with open('generated_data/intermediate/all_sectors.txt') as file:
   sectors_list = [l.rstrip() for l in file.readlines()]
# sectors = df.loc[df['location'].str.contains(' ')]['location']
# sectors_list = list(sectors)
metric = 'durations' #or distances
with open(f'generated_data/{metric}_matrix.csv', 'w') as file:
    metric_unit = 'seconds' if metric == 'durations' else 'metres' if metric == 'distances' else 'unknown'
    file.write(f'"{metric} (in {metric_unit}. down rows are origins, right columns are destinations. source: Open Route Service)",' + 
        ','.join(sectors_list) + '\n')

    assert(len(js[metric]) == len(sectors_list))

    for i in range(len(sectors_list)):
        row = js[metric][i]
        file.write(sectors_list[i] + ',' )
        file.write(','.join([str(e) for e in row]))
        file.write('\n')

