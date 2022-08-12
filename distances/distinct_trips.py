

f1p = 'C:/SFTP/san-ssapfs/edge/home/chaudhup/Network_Rail/nr_module_1_2021.csv'
tripset = set()
i = 0
with open (f1p) as file:
    _ = next(file)
    for line in file:
        split = line.split('|')
        loc_ch = split[3]
        loc_m = split[5]
        
        tripset.add(','.join((loc_ch, loc_m)))
        if i % 1000000==0:
            print(i)
        # if i>= 10:
        #     break
        i +=1

with open('generated_data/distinct_trips.csv', 'w') as outf:
    outf.write('cardholder_location,merchant_location\n')
    for trip in tripset:
        outf.write(trip+'\n')