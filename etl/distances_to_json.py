import json
import csv

from dimensions import NULL_CELL, _distance_dict_key

# JSON loading a dict is faster than CSV loading in Python because it's built-in and Python is slow for anything else

def csv_to_dict(fpath):

    with open(fpath,'r') as file: 
        csv_reader = csv.reader(file)

        def dict_key(origin, dest) -> str:
            return _distance_dict_key(origin, dest)
            # return ','.join((origin, dest))

        def line_to_dict(line):
            if (len(line) >=7 ):
                try:
                    # get 6th element because that's where the distance is in this case
                    i6 = int(line[6])
                    if i6 < 0:
                        i6 = NULL_CELL
                    return dict_key(line[0], line[1]), str(i6)
                except ValueError as ve:
                    return dict_key(line[0], line[1]), NULL_CELL
            elif (len(line) >= 2):
                return dict_key(line[0], line[1]), NULL_CELL
            else:
                return dict_key(NULL_CELL, NULL_CELL), NULL_CELL

        distances_dict = dict(line_to_dict(line) for line in csv_reader)
        return distances_dict

def dict_to_json(dic, outfpath):
    with open(outfpath, 'w') as outfile:
        outfile.write(json.dumps(dic))


def etl(infpath, outjson = 'generated_data/distances_dict.json'):
    """
    Convert an existing driving distances CSV to a json file.
    CSV should have origin at element 0, destination at element 1, and distance at element 6
    """
    dic = csv_to_dict(infpath)
    dict_to_json(dic, outjson)