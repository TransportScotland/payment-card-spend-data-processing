import pandas as pd
import os 
pd.options.display.max_columns = 20

for file in os.listdir('wh2'):
    print(file)
    print(pd.read_csv(os.path.join('wh2', file)))