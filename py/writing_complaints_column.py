
import pandas as pd
res = all_comp.map(lambda x: x)
col_name = pd.DataFrame({'column_name':res.take(1)[0].split(',')})
col_name.to_csv('./Analyzing_Noise_in_NYC/output/complaints_column.csv')