from timeit import timeit

def temp():
    rl = ['ABCDEFGHIJKLMNOPQRSTUVWXYZ']
    with open('temp.csv', 'w') as file:
        file.writelines([e+'\n' for e in rl]) # fastest
        # file.writelines([f'{e}\n' for e in rl])
        # file.write('\n'.join(rl) + '\n')

def temp2():
    # only takes about 0.15 seconds on 200MB,
    # so most of the time is probably on reading not writing
    with open('data/file1.csv') as file:
        for line in file:
            # a= line
            # line + '\n'
            pass

import pandas as pd
import sqlalchemy

def temp3():
    con = sqlalchemy.create_engine('mysql://temp_user:password@localhost:3306/sgov')
    # con = 
    # df = pd.read_sql('select sum(pan_cnt), cl.district as origin, ml.sector as destination from fact1 inner join (select id,sector from location) ml on fact1.merchant_id=ml.id inner join (select id, district from location) cl on fact1.cardholder_id=cl.id group by destination, origin;', con=con)
    
    # group and sum are significantly faster in pandas than sql (2.5s vs 9s)
    # but will need to check memory limitations. Then either check out dask or just use sql
    
    df = pd.read_sql('select pan_cnt, cl.district as origin, ml.sector as destination from fact1 inner join (select id,sector from location) ml on fact1.merchant_id=ml.id inner join (select id, district from location) cl on fact1.cardholder_id=cl.id;', con=con)
    sum = df.groupby(by=['destination', 'origin']).sum('pan_cnt')
    df = pd.DataFrame(sum)
    print(df)

def time(fnn, num = 1):
    t = timeit(f"{fnn}()", f"from __main__ import {fnn}", number=num)
    print(t)

if __name__ == '__main__':
    time('temp3', 1)


