# import sqlitedict

# maybe save into csv file in batches and save each batch to db
# to limit space used

#currently just a proof of concept.
# takes about 1.1s per 1 million rows when handling only time
# saving in batches is faster (though not crazy - 0.93s vs 1.09s)

import time 

def stuff():

    with open('data/module1_sample.csv') as infile:
        with open('data/module1_tl.csv', 'w') as outfile:

            mtoq = {
                    m: int((m-1)/3)+1 for m in range(1,13)
                }

            qtoq =  {
                    'Jan': 1, 'Apr': 2, 'Jul': 3, 'Oct': 4
                }
            _ = next(infile)
            to_save_arr = []
            for line in infile:
                split = line[:-1].split('|')
                time = split[0]
                out = split
                out.append(time[0:4] if len(time) <= 8 else time[4:6])
                out.append(time[4:6] if len(time) <= 8 else "")
                out.append(str(mtoq[int(time[4:6])] if len(time) <= 8 else qtoq[time[0:3]]))
                
                to_save_arr.append(out)
                # to_save += '|'.join(out)+ '\n'
                outfile.write('|'.join(out)+ '\n') 

            # outfile.write('\n'.join('|'.join(line) for line in to_save_arr))

tstart = time.perf_counter()


for i in range(1000):
    stuff()

print(time.perf_counter() - tstart)