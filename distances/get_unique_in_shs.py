import json
import time
import pandas as pd
import requests


# it may be faster to just get a matrix of all of the locations
# than just the individual journeys

shs_df = pd.read_csv('other_data/SHS_data_merged_19200.csv')
_ = ['PO1', 'PD1']

unique_locs = pd.concat([shs_df['PO1'], shs_df['PD1']]).unique()
unique_locs = pd.Series(unique_locs, name='shs_locs')
print(unique_locs)
print(len(unique_locs))

postcode_df = pd.read_csv('other_data/open_postcode_geo_scotland.csv', header=None)
postcode_df = postcode_df.rename(columns={0: 'postcode', 7:'lat', 8:'lng', 9:'postcode_no_space'})
print(postcode_df)

# geoc = pd.DataFrame.merge(
#     postcode_df[['postcode_no_space', 'lat', 'lng']], 
#     unique_locs, 
#     left_on='postcode_no_space', 
#     right_on='shs_locs', 
#     how='inner'
#     ).drop('shs_locs', axis=1)
# print(geoc)


geoc = shs_df[['PO1', 'PD1']].merge(
    postcode_df[['postcode_no_space', 'lat', 'lng']],
    how = 'inner',
    left_on='PO1',
    right_on='postcode_no_space',
).drop(
    'postcode_no_space', 
    axis=1,
).rename(
    columns={'lat':'origin_lat', 'lng' : 'origin_lng'},
).merge(
    postcode_df[['postcode_no_space', 'lat', 'lng']],
    how='inner',
    left_on='PD1',
    right_on='postcode_no_space',
).drop(
    'postcode_no_space', 
    axis = 1
).rename(
    columns={'lat':'destination_lat', 'lng':'destination_lng'},
)

print(geoc)

# save the geo coords. also useful for ordering when referring back
geoc.to_csv('generated_data/intermediate/shs_geoloc.csv')

# req={'locations':geoc.loc[:10, ['lng', 'lat']].to_numpy().tolist()}

# def get_dist(row):
    # print(row)

# temp = geoc[:10].apply(get_dist,)


# not the cleanest solution, assumes equal number of elements in req csv and saved
# def directions_req_file(_):
#     fpath = 'generated_data/intermediate/ors_response_shs.json'
#     with open(fpath) as file:
#         js = json.loads(file.read())
#         for item in js:
#             yield item
#     pass

def directions_req_gh(start_lat, start_lng, end_lat, end_lng):
    url_host_port = '34.78.207.187:8989'

    #profiles: pt, 
    req_json = {
        # 'point': [[-2.470357, 56.713944],[-2.096076, 57.144369]]
        'points': f'[[{start_lng},{start_lat}], [{end_lng}, {end_lat}]]',
        # 'instructions': False,
        'profile': 'pt',
        'pt.earlist_departure_time': '2021-02-10T09:00:00.000Z',
    }
    url = f'http://{url_host_port}/route' \
        f'?point={start_lat},{start_lng}' \
        f'&point={end_lat}, {end_lng}' \
        f'&profile=pt' \
        f'&pt.earliest_departure_time=2021-02-14T09:00:00.000Z'
    res = requests.get(url)
    return res
    # summary time includes wait time, will need to calc time from 0th leg to last leg


def directions_req(start_lat, start_lng, end_lat, end_lng):
    # import requests
    # res = requests.get('http://localhost:8080/ors/v2/directions/driving-car?start')
    url_base = 'http://localhost:8080/ors/v2/directions/driving-car'
    # url = f'{url_base}?start={start_lng},{start_lat}&end={end_lng},{end_lat}'
    # res = requests.get(url)
    req_json = {
        'coordinates':[[start_lng, start_lat],[end_lng, end_lat]],
        'elevation': True,
        'instructions': False,
        'geometry': False,
    }
    res = requests.post(url_base, json=req_json)
    return res

# USE_SAVED_RESPONSES = True
# USE_SAVED_RESPONSES = False

def directions_req_numbered(row):
    # if (USE_SAVED_RESPONSES):
    #     return next(directions_req_file(row))

    return directions_req_gh(
        start_lat = row[2], 
        start_lng = row[3],
        end_lat = row[4], 
        end_lng = row[5],
    )

max_requests = None # set to None to do all
lng_lat_list = geoc[:max_requests].to_numpy().tolist()
# lng_lat_list = geoc.to_numpy().tolist()
print(lng_lat_list)
# temp = [x for x in lng_lat_list]
# for row in lng_lat_list:
#     res = directions_req(
#         start_lat = row[2], 
#         start_lng = row[3],
#         end_lat = row[4], 
#         end_lng = row[5],
#     )

def directions_rn_summary(row):
    res = directions_req_numbered(row)
    return res.json()['routes'][0]['summary']

timer1000 = time.time()
def get_response_with_counter(row):
    global count
    if (count % 1000 == 0):
        global timer1000
        print(f'{count}, {time.time() - timer1000}s')
        timer1000 = time.time()
    count += 1
    return directions_req_numbered(row)


def get_all_responses_json(lng_lat_list):
    time0 = time.time()
    out = [get_response_with_counter(row).json() for row in lng_lat_list]
    time1 = time.time()
    print(f'took {time1-time0}s to get {max_requests if max_requests else "all"} travel durations')
    return out

def save_all_responses(responses, outfpath):
    with open(outfpath, 'w') as outfile:
        outfile.write(json.dumps(responses))

def read_all_responses(infpath):
    with open(infpath) as infile:
        return json.loads(infile.read())



def directions_summary_keep_postcodes(postcodes,row, file = None):
    # global count
    # count += 1
    # if (count % 1000 == 0):
    #     print(count)
    # res = directions_req_numbered(row)
    res = row
    po = postcodes[0]
    pd = postcodes[1]
    if file:
        file.write(json.dumps(res.json()) + ',')
    try: 
        if True:
            s = res['routes'][0]['summary']
        else:
            s = res.json()['routes'][0]['summary']
    except KeyError as ke:
        # print(row[0])
        # print(row[1])
        # print(f'KeyError: {ke}')
        # print(res)
        # print(res.text)

        # WHYYYY
        #  KeyError is not "routes" but "'routes'" with the extra quotes for some reason
        if (str(ke) == "'routes'"):
            # no route found
            return (po,pd, 'no route found', '', '', '')
        else:
            #unknown error
            return (po,pd, f'"KeyError: {ke}"', res, None, None)

    try:
        distance = s['distance']
        duration = s['duration']
    except KeyError as ke:
        if (po == pd):
            return [po,pd,0,0,0,0]
        else:
            return [po, pd, f'"KeyError: {ke}"', res, None, None]
    ascent = s.get('ascent', 0)
    descent = s.get('descent', 0)

    # try:
    #     ascent = s['ascent']
    # except KeyError:
    #     ascent = 0
    
    # try:
    #     descent = s['descent']
    # except KeyError:
    #     descent = 0


    return [po, pd, distance, duration, ascent, descent]
    # except KeyError as ke:
    #     kestr = str(ke)
    #     if (kestr == 'distance'):
    #     else:

def directions_summary_gh(postcodes, row):
    po = postcodes[0]
    pd = postcodes[1]
    
    legs = row['paths'][0]['legs']
    start_time = legs[0]['departure_time']
    end_time = legs[-1]['arrival_time']
    return (po, pd, start_time, end_time, None, None)
count=0

service_name = 'gh'
fp = f'generated_data/intermediate/{service_name}_response_shs.json'
USE_SAVED_RESPONSES = True
USE_SAVED_RESPONSES = False
if not USE_SAVED_RESPONSES:
    responses_raw = get_all_responses_json(lng_lat_list)
    save_all_responses(responses_raw, fp)
    del responses_raw
responses_raw = read_all_responses(fp)
response_summary_fn = {
    'gh': directions_summary_gh,
    'ors': directions_summary_keep_postcodes}[service_name]
responses = [response_summary_fn((lnglat[0], lnglat[1]), row) 
    for row, lnglat in zip(responses_raw, lng_lat_list)]

# if (USE_SAVED_RESPONSES):
#     responses = [directions_summary_keep_postcodes(row) for row in lng_lat_list]
# else:
#     with open('generated_data/intermediate/ors_response_shs.json', 'w') as outfile:
#         responses_raw_json = [directions_req_numbered(row).json() for row in lng_lat_list]
#         outfile.write(json.dumps(responses_raw_json))
        # outfile.write('{"list":')
        # responses = [directions_summary_keep_postcodes(row, outfile) for row in lng_lat_list]
        # outfile.write('}')
# print(responses)

for res in responses:
    if (type(res[3]) == requests.Response):
        print(res[0])
        print(res[1])
        print(res[2])
        print(res[3])
        print(res[3].text)



with open(f'generated_data/{service_name}_driving_shs_data.csv', 'w') as outfile:
    outfile.write('PO1,PD1,distance(m),durations(s),asecnd(m),descent(m)\n')
    outfile.writelines([','.join([str(v) for v in r]) + '\n' for r in responses])



# for res in responses:
#     js = json.loads(res.text)
#     # print(js)
#     # print(js.keys())
#     summary = js['routes'][0]['summary']
#     # keys distance, duration, ascent, descent
#     print(summary)
#     # # the following is for GET endpoint
#     # distance = js['features'][0]['properties']['summary']['distance']
#     # duration = js['features'][0]['properties']['summary']['duration']
#     # ascent   = js['features'][0]['properties']['ascent']
#     # descent  = js['features'][0]['properties']['descent']
#     # for v in [distance, duration, ascent, descent]:
#     #     print(f'{v.__name__}: v')

    
    
# # print(temp)