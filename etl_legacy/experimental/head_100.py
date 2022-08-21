with open('C:/SFTP/san-ssapfs/edge/home/chaudhup/Network_Rail/nr_module_1_2021.csv') as file:
    i = 0
    with open('temp_head100.csv', 'w') as outfile:
        for line in file:
            # print(line)
            # line = line.replace(',', '{comma}')
            # line = line.replace('|', ',')
            outfile.write(line)
            i+=1
            if i > 100:
                break