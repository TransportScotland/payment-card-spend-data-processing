import json
import csv

from file1 import NULL


def csv_to_dict(fpath):
    import csv
    # global sqlitecon
    # con = sqlitecon
    # cur = con.cursor()
    # cur.execute("CREATE TABLE distance (origin varchar(6), \
    #     destination varchar(6), driving_seconds int, \
    #     primary key(origin, destination));")

    with open(fpath,'r') as file: # `with` statement available in 2.5+
        # csv.DictReader uses first line in file for column headings by default
        csv_reader = csv.reader(file)

        def dict_key(origin, dest) -> str:
            return ','.join((origin, dest))

        def line_to_dict(line):
            if (len(line) >=7 ):
                try:
                    i6 = int(line[6])
                    if i6 < 0:
                        i6 = NULL
                    return dict_key(line[0], line[1]), str(i6)
                except ValueError as ve:
                    return dict_key(line[0], line[1]), NULL
            elif (len(line) >= 2):
                return dict_key(line[0], line[1]), NULL
            else:
                return dict_key(NULL, NULL), NULL

        distances_dict = dict(line_to_dict(line) for line in csv_reader)
        return distances_dict
        # for chunk in read_distance_csv_chunk(csv_reader, 1000):
        # # dr = csv.DictReader(file) # comma is default delimiter
        # # to_db = [(i['col1'], i['col2']) for i in dr]

        #     cur.executemany("INSERT INTO distance (origin, destination, driving_seconds) VALUES (?, ?);", chunk)
        #     con.commit()
    # con.close()

def dict_to_json(dic, outfpath):
    with open(outfpath, 'w') as outfile:
        outfile.write(json.dumps(dic))


def etl(infpath, outjson = 'generated_data/distances_dict.json'):
    dic = csv_to_dict(infpath)
    dict_to_json(dic, outjson)