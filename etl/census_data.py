import pandas as pd
import shared_funcs
import os

temp_pop_dict = {}

def each_row(row, _):
    # loc_dict = shared_funcs.dicts['location']
    loc_raw = row[0]
    population = int(row[1].replace(',', ''))

    #turn "AB12 3 (part) Aberdeen City" to just "AB12 3" (do nothing if shorter)
    loc = loc_raw if len(loc_raw) <=6 else ' '.join(loc_raw.split(' ', 2)[:2])
    temp_pop_dict[loc] = population
    if loc in temp_pop_dict:
        temp_pop_dict[loc] += population
    # print(f'handling loc {loc}, pop {population}')
    pass

def add_up_sectors(dic):
    sectors = set(dic.keys())
    for sector in sectors:
        _, district, area, _ = shared_funcs.postcode_sector_to_loc_list(sector)
        
        if district not in dic:
            dic[district] = 0
        dic[district] += dic[sector]

        if area not in dic:
            dic[area] = 0
        dic[area] += dic[sector]


def etl(data_folder, file_name):
    # shared_funcs.ensure_dim('location')

    #probably don't do the shared_funcs function here
    shared_funcs.read_and_handle(os.path.join(data_folder, file_name), each_row, None, skip_n_rows=5, skip_after_0_empty=True)
    # print(temp_pop_dict)
    

    print (pd.DataFrame(zip(temp_pop_dict.keys(), temp_pop_dict.values()), columns=['loc', 'population']))
    add_up_sectors(temp_pop_dict)
    print (pd.DataFrame(zip(temp_pop_dict.keys(), temp_pop_dict.values()), columns=['loc', 'population']))

if __name__ == '__main__':
    etl('other_data/', 'KS101SC.csv')
