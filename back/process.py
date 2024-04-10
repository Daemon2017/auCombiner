import os
import time

import numpy as np
import pandas as pd

data_dir = 'data'

dfs = []

start_time = time.time()
data_path = os.path.join(data_dir)
for folder in os.listdir(data_path):
    folder_path = os.path.join(data_dir, folder)
    if os.path.isdir(folder_path):
        for file in os.listdir(data_path):
            file_path = os.path.join(data_dir, folder, file)
            if os.path.isfile(file_path):
                dfs.append(pd.read_csv(file_path, dtype=str))

merged_df = []
for i, df in enumerate(dfs):
    if i == 0:
        merged_df = df.rename(columns={'RESULT': 'RESULT_{0}'.format(i)})
    else:
        renamed_df = df.rename(columns={'RESULT': 'RESULT_{0}'.format(i)})
        merged_df = pd.merge(merged_df, renamed_df, on=['RSID', 'CHROMOSOME', 'POSITION'], how='outer')

merged_support_df = merged_df[['RSID', 'CHROMOSOME', 'POSITION']].copy()
merged_values_df = merged_df.drop(columns=['RSID', 'CHROMOSOME', 'POSITION']).copy()
mode_df = merged_values_df.mode(axis=1)
merged_support_df['RESULT'] = np.where(mode_df.count(axis=1) == 1, mode_df[0], '--')
merged_support_df.to_csv(os.path.join(data_dir, 'merged.csv'), index=False)
print("--- %s seconds ---" % (time.time() - start_time))
