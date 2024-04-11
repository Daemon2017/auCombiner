import os

import numpy as np
import pandas as pd

data_dir = 'data'
support_columns = ['RSID', 'CHROMOSOME', 'POSITION']

dfs = []
data_path = os.path.join(data_dir)
for file in list(filter(lambda f: f.lower().endswith('.csv'), os.listdir(data_path))):
    file_path = os.path.join(data_dir, file)
    if os.path.isfile(file_path):
        dfs.append(pd.read_csv(file_path, sep=',', dtype=str))

if len(dfs) > 1:
    merged_df = []
    for i, df in enumerate(dfs):
        if i == 0:
            merged_df = df.rename(columns={'RESULT': 'RESULT_{0}'.format(i)})
        else:
            renamed_df = df.rename(columns={'RESULT': 'RESULT_{0}'.format(i)})
            merged_df = pd.merge(merged_df, renamed_df, on=support_columns, how='outer')
    merged_support_df = merged_df[support_columns].copy()
    merged_values_df = merged_df.drop(columns=support_columns).copy()

    mode_df = merged_values_df.mode(axis=1)
    merged_support_df['RESULT'] = np.where(mode_df.count(axis=1) == 1, mode_df[0], '--')
    merged_support_df.to_csv('combined.csv', sep=',', index=False, header=True)
else:
    print('There must be more than 1 data sets!')
