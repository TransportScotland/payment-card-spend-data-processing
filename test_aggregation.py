# a quick script to check if any row contains aggregate information 
# from sector to district or area.
# Doesn't check if all of them are aggregated, only at least one


import etl.shared_funcs as shared_funcs

dic = {} #cardholder sector to set of all merchants

def each_row(row, _):
    cardholder_loc = row[3]
    merchant_loc = row[5]

    # create set if not exist
    if cardholder_loc not in dic:
        dic[cardholder_loc] = set()

    #add whole merchant location to set
    dic[cardholder_loc].add(merchant_loc)
    # merchant_set = dic[cardholder_loc]
    # merchant_set.add(merchant_loc)

shared_funcs.read_and_handle("file1_aggtest.csv", each_row, None)

for cardholder_loc, merchant_set in dic.items():
    for merchant_loc in merchant_set:
        if merchant_loc[:-2] in merchant_set:
            print(f'Cardholder location "{cardholder_loc}" already contains aggregate data for merchant district "{merchant_loc[:-2]}"')
        if merchant_loc[:-3] in merchant_set:
            print(f'Cardholder location "{cardholder_loc}" already contains aggregate data for merchant area "{merchant_loc[:-3]}"')
