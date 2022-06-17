# generate a matrix/table where 
# horizontal headers are origin locations (columns)
# vertical "headers" are destination locations (rows)
# cells are summarised pan_cnt or another measure (txn_cnt or txn_gbp_amt)
#
# author: Transport Scotland

import pandas as pd
pd.options.display.max_columns = 20

# ideally the locations could be sorted in a way that places close-by locations next to each other
# alternatively alphabet sort works for now bc it groups same postcode areas together


fact_df = pd.read_csv('wh/fact1.csv')
print(fact_df)
loc_df = pd.read_csv('wh/location.csv')
print(loc_df)



# select sum(pan_cnt) 
# from LOC inner join FACT on merchant inner join LOC on cardholder 
# group by (m.district, c.district)
#
# then pivot somehow - might be best using pandas actually instead of SQL
df = pd.merge(fact_df, loc_df,'inner', left_on='merchant_id', right_on='id')
df = pd.merge(df, loc_df, 'inner', left_on='cardholder_id', right_on='id', suffixes=['_m', '_c'])
df = df.rename(columns={'district_c': 'origin', 'sector_m': 'destination'})
print(df)
df = df.groupby(by=['destination', 'origin'])
df = df['pan_cnt'].sum()
df = pd.DataFrame(df) # was just a series before - wouldn't pivot

print(df)
df = pd.pivot_table(df, columns='origin', index='destination', values='pan_cnt')
print(df)

df.to_html('out/temp.html')

df.index.rename('Count of people from origin (columns) spending money at destination (rows)', inplace=True)
df.to_csv('out/origin-destination matrix (on pan_cnt).csv')

