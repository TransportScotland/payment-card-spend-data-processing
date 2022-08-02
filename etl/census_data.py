import csv
import shared_funcs


def each_row(row, population_dict):
    """
    Adds data from row[1] to population_dict with key from row[0], collecting any split up values together.
    
    Parameters:
        row: tuple or list with postcode sector at element 0 and a number at element 1
        population_dict: Dictionary which will be modified to add the new row of data
    """
    loc_raw = row[0]
    population = int(row[1].replace(',', ''))

    #turn "AB12 3 (part) Aberdeen City" to just "AB12 3" (do nothing if shorter)
    loc = loc_raw if len(loc_raw) <=6 else ' '.join(loc_raw.split(' ', 2)[:2])

    # if already in
    if loc in population_dict:
        population_dict[loc] += population # add to existing
    else:
        population_dict[loc] = population # create new dict element with this value


def add_up_sectors(dic):
    """
    Add up the values in each sector to a district and area, modifying the input dictionary to add the new values

    Parameters:
        dic: dictionary of {postcode_sector: value_to_add_up}. Will be modified.
    """

    # todo consider instead returning a new dict rather than modifying current one.
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
    """Changes dict from {location: population} to {location: (location, population, random_index)}"""
    for i, (loc, population) in enumerate(dic.items()):
        dic[loc] = (loc, population, i)

def get_filled_population_dict(fpath):
    """
    Reads the file at fpath and fills a dictionary with the values in it useing census_data.each_row. 
    The file is expected to be in the format that the Scottish census data is in, 
    with 6 non-useful rows before the actual data and the end of data specified by an empty line (or end of file)
    """
    population_dict = {}
    with open (fpath) as file:
        # skip first 6 rows - meta info plus a row of total scotland data
        for _ in range(6):
            _ = next(file)

        # for each row in the remainder of the CSV
        csvreader = csv.reader(file)
        for row in csvreader:
            if not row[0]:
                # end loop if first element empty (the csv contains a few empty rows after the data before some meta information)
                break
            # add the population value of the current row to the dictionary
            each_row(row, population_dict)
    return population_dict


def save_to_sql(population_dict):
    """
    Saves the population_dict to a sql database defined in shared_funcs.get_sqlalchemy_con.

    Parameters:
        population_dict: dict of {location: population}
    Returns:
        A pandas DataFrame similar to the one saved to the database, for easier printing.
    """

    # pandas is slow but the census data currently isn't big enough to matter
    import pandas as pd
    import table_info

    table_headers= table_info.headers_dict['census'] # location, population, id

    # convert dict to df with rows like (id, location, population), taking the column names from table_info headers
    df = pd.DataFrame(data=zip(population_dict.keys(), population_dict.values()), columns=table_headers[:2])

    # save to sql, renaming the index to the index column specified in table_info headers
    df.to_sql('census', shared_funcs.get_sqlalchemy_con(), index_label=table_headers[-1], if_exists='replace')
    return df


def etl(fpath):
    """Load census data into database as its own table"""

    # process the input file to get a dict of {postcode_sector: total_population}
    population_dict = get_filled_population_dict(fpath)

    # add up sector number to district and area values
    add_up_sectors(population_dict)

    # save to the SQL database
    pop_df = save_to_sql(population_dict)
    print(pop_df)

# consider doing this before putting all the locations into sql - or just have the census dict be the main location table
def move_to_locations():
    eng = shared_funcs.get_sqlalchemy_con()
    with eng.connect() as con:
        con.execute("alter table location add column population int;")
        con.execute("update location l left join census c on l.sector = c.location set l.population = c.population;")


# call the etl function if this file is run as a stand-alone program
if __name__ == '__main__':
    etl('other_data/KS101SC.csv')
