# only works for small databases that can fit into memory

import pandas as pd
import os
import sys

# this should not be done but it's the easiest way I've found to import from a sibling directory
sys.path.append('etl')
import shared_funcs
sys.path.append('..')
con = shared_funcs.get_sqlalchemy_con()

def load_and_save(name, folder):
    df = pd.read_sql_table(name, con)
    os.makedirs(folder, exist_ok=True)
    df.to_csv(f'{os.path.join(folder, name)}.csv')

all_tables_res = con.execute('show tables;').fetchall()
all_tables = [e[0] for e in all_tables_res] # un-tuple

for table in all_tables:
    load_and_save(table, folder= 'temp_db_csv')
