import pandas as pd
pd.set_option('display.max_columns', 20)

data_dir = '../other_data/'

def bad_line(what):
    # print('bad line')
    # print(what)
    # return what[:4]
    if (len(what) == 6):
        return what[:4]
    else:
        print('bad line:')
        print(what)

# csvcols = ['PO1', 'PD1', 'time_s', 'time_s_added_up', 'exception', 'exc2']
df_car = pd.read_csv(data_dir + 'SHS_GH_car.csv', on_bad_lines='warn')
df_pt = pd.read_csv(data_dir + 'SHS_GH_pt.csv', on_bad_lines=bad_line, engine='python')
df_foot = pd.read_csv(data_dir + 'SHS_GH_foot.csv', on_bad_lines='warn')
df_bike = pd.read_csv(data_dir + 'SHS_GH_bike.csv', on_bad_lines='warn')

ogdfs = [df_car, df_pt, df_foot, df_bike]
ogdfs_profile_names = ['car', 'pt', 'foot', 'bike']

ogdfs_millis = ogdfs
for ogdf, profile in zip(ogdfs, ogdfs_profile_names):
    # ogdf.iloc[:, 2:4] = ogdf.iloc[:,2:4] / 1000
    ogdf[f'gh_{profile}_sec'] = (ogdf[f'time_{profile}_added_up']/1000).astype(int).rename(f'gh_{profile}_sec')
    # ogdf = ogdf[['PO1', 'PD1', f'gh_{profile}_sec']]
    # ogdf[3] = ogdf[3] / 1000
ogdfs = [ogdf[['PO1', 'PD1', ogdf.columns[-1]]] for ogdf in ogdfs]
print(ogdfs)

# df_all = pd.DataFrame([df[ f'time_{profile}_added_up'] for df, profile in zip(ogdfs, ogdfs_profile_names)])
# df_all = pd.concat(ogdfs, axis=1)
oncols = ['PO1', 'PD1']
df_all = ogdfs[0
    ].merge(ogdfs[1], on=oncols, how = 'outer'
    ).merge(ogdfs[2], on=oncols, how = 'outer' 
    ).merge(ogdfs[3], on=oncols, how = 'outer' 
    ).drop_duplicates()

# df_all = pd.concat(ogdfs, axis=1)


ogdfs_col_names = [f'{profile}_time_s' for profile in ogdfs_profile_names]
# print(df_all)
# df_all.loc[ogdfs_profile_names] = df_all.loc[ogdfs_profile_names] / 1000000
# df_all[ogdfs_profile_names] = df_all[ogdfs_profile_names].rename(ogdfs_col_names)

# df_all.to_csv('generated_data/SHS_GH_time')
df_shs = pd.read_csv('other_data/SHS_data_merged_19200.csv')

print(df_all)
print(df_all.columns)
print()
print(df_shs)
print(df_shs.columns)

# df_shs_only_time = df_shs[['PO1', 'PD1', 'car_freeflow_TravelTime_sec', 'car_InTraffic_TravelTime_sec',
#         'PT_TravelTime_sec', 'Walk_TravelTime_sec','Bicycle_TravelTime_sec',]]
joined  = pd.merge(df_shs, df_all, how='outer', on=['PO1','PD1'])

print (joined)
# print(len(joined))
joined.to_csv('generated_data/shs_compare.csv', index=False, float_format='%.0f')