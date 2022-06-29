import csv
import shared_funcs

temp_pop_dict = {}

def each_row(row):
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

def change_dict_structure(dic):
    for i, (loc, population) in enumerate(dic.items()):
        dic[loc] = (loc, population, i)


def etl(fpath, table_name = 'census'):
    # shared_funcs.ensure_dim('location')

    #probably don't do the shared_funcs function here
    # shared_funcs.read_and_handle(os.path.join(data_folder, file_name), each_row, None, skip_n_rows=5, skip_after_0_empty=True)
    
    with open (fpath) as file:
        # skip first 6 rows - meta info plus a row of total scotland data
        for _ in range(6):
            _ = next(file)
        # shared_funcs.batch_process_opened_file(file, each_row, table_name)
        csvreader = csv.reader(file)
        for row in csvreader:
            if not row[0]:
                # if empty first element
                break
            each_row(row = row)
            
    # print(temp_pop_dict)

    # print (pd.DataFrame(zip(temp_pop_dict.keys(), temp_pop_dict.values()), columns=['loc', 'population']))
    add_up_sectors(temp_pop_dict)
    change_dict_structure(temp_pop_dict)

    shared_funcs.dicts['census'] = temp_pop_dict
    
    shared_funcs.save_dim('census', dbcon=None, unique_index='location')

    # print (pd.DataFrame(zip(temp_pop_dict.keys(), temp_pop_dict.values()), columns=['loc', 'population']))

if __name__ == '__main__':
    etl('other_data/KS101SC.csv')
