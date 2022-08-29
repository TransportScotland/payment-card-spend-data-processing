# generate a matrix/table where 
# horizontal headers are origin locations (columns)
# vertical "headers" are destination locations (rows)
# cells are summarised pan_cnt or another measure (txn_cnt or txn_gbp_amt)
#
# Transport Scotland

import pandas as pd
import sqlalchemy
pd.options.display.max_columns = 20

# ideally the locations could be sorted in a way that places close-by locations next to each other
# alternatively alphabet sort works for now bc it groups same postcode areas together


# 'select sum(pan_cnt), cl.district as origin, ml.sector as destination
#  from fact1 inner join 
#     (select id,sector from location) ml 
#     on fact1.merchant_id=ml.id 
#  inner join 
#     (select id, district from location) cl 
#     on fact1.cardholder_id=cl.id 
#  group by destination, origin;
#
# ^ this is ~4x slower than group and sum with pandas, so only use 
# when the table doesn't fit into memory and the dask library doesn't help
#
# then pivot somehow - easiest with pandas
# MySQL doesn't support pivot (Microsoft SQL Server does)


# attribute = 'pan_cnt'
attribute = 'txn_gbp_amt'
# attribute = 'txn_cnt'

con = sqlalchemy.create_engine('mysql://temp_user:password@localhost:3306/sgov')
df = pd.read_sql(f'select {attribute}, cl.district as origin, ml.sector as destination from fact1 inner join (select id,sector from location) ml on fact1.merchant_id=ml.id inner join (select id, district from location) cl on fact1.cardholder_id=cl.id;', con=con)
sum = df.groupby(by=['destination', 'origin']).sum(attribute)
df = pd.DataFrame(sum)



print(df)
df = pd.pivot_table(df, columns='origin', index='destination', values=attribute)
print(df)

# df.to_html('out/temp.html')

# select str from dict, or set it to just the attribute if not found
top_corner_str = {
    'pan_cnt':
        'Number of people from origin (columns) spending money at destination (rows)',
    'txn_gbp_amt':
        'Total GBP spend by those from origin (columns) at destination (rows)',

}.get(attribute, attribute+ '. destination is rows (vertical headers), origin is column (horizontal headers)') 


dfcsv = df.rename_axis(index=top_corner_str)
dfcsv.to_csv('out/origin-destination matrix (on pan_cnt).csv')



# csv version:

# fact_df = pd.read_csv('wh2/fact1.csv')
# print(fact_df)
# loc_df = pd.read_csv('wh/location.csv')
# print(loc_df)

# df = pd.merge(fact_df, loc_df,'inner', left_on='merchant_id', right_on='id')
# df = pd.merge(df, loc_df, 'inner', left_on='cardholder_id', right_on='id', suffixes=['_m', '_c'])
# df = df.rename(columns={'district_c': 'origin', 'sector_m': 'destination'})
# print(df)
# df = df.groupby(by=['destination', 'origin'])
# df = df['pan_cnt'].sum()
# df = pd.DataFrame(df) # was just a series before - wouldn't pivot