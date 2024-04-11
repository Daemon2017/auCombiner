from io import StringIO

import numpy as np
import pandas as pd
from flask import Flask, request, Response
from flask_cors import CORS
from waitress import serve

app = Flask(__name__)
cors = CORS(app)

data_dir = 'data'
support_columns = ['RSID', 'CHROMOSOME', 'POSITION']


@app.route('/combine', methods=['POST'])
def combine():
    dfs = []
    files = request.files.getlist("csv_files")
    for file in files:
        csv_string = file.read().decode('utf-8')
        dfs.append(pd.read_csv(StringIO(csv_string), sep=',', dtype=str))

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
    return Response(merged_support_df.to_csv(sep=',', index=False, header=True), mimetype='text/csv')


if __name__ == '__main__':
    print('auCombiner ready!')
    serve(app,
          host='0.0.0.0',
          port=8080)
