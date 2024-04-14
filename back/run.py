import os
import zipfile

import boto3
import numpy as np
import pandas as pd
from flask import Flask, Response, request
from flask_cors import CORS
from waitress import serve

from utils import get_keys

app = Flask(__name__)
cors = CORS(app)

bucket_name = 'aucombiner'
file_name = 'data.zip'
support_columns = ['RSID', 'CHROMOSOME', 'POSITION']

access_key_id, secret_access_key = get_keys('api-key.yaml')
client = boto3.client(
    's3',
    aws_access_key_id=access_key_id,
    aws_secret_access_key=secret_access_key,
    endpoint_url='https://storage.yandexcloud.net',
    region_name='ru-central1',
)


@app.route('/put_object', methods=['GET'])
def put_object():
    rq_id = request.headers['key']
    signed_url = client.generate_presigned_url(
        'put_object',
        Params={
            'Bucket': bucket_name,
            'Key': rq_id + '.zip'
        },
        ExpiresIn=360
    )
    return Response(signed_url, mimetype='text/plain')


@app.route('/combine', methods=['POST'])
def combine():
    rq_id = request.headers['key']
    if not os.path.exists(rq_id):
        os.makedirs(rq_id)
    client.download_file(bucket_name, rq_id + '.zip', os.path.join(rq_id, file_name))
    with zipfile.ZipFile(os.path.join(rq_id, file_name), 'r') as zip:
        zip.extractall(rq_id)
    try:
        os.remove(os.path.join(rq_id, file_name))
    except OSError:
        pass
    client.delete_object(Bucket=bucket_name, Key=rq_id + '.zip')

    dfs = []
    for file in list(filter(lambda f: f.lower().endswith('.csv'), os.listdir(rq_id))):
        file_path = os.path.join(rq_id, file)
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
        merged_support_df.to_csv(os.path.join(rq_id, 'combined.csv'), sep=',', index=False, header=True)

        client.upload_file(os.path.join(rq_id, 'combined.csv'), bucket_name, rq_id + '.csv')
        signed_url = client.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': bucket_name,
                'Key': rq_id + '.csv'
            },
            ExpiresIn=360
        )
        return Response(signed_url, mimetype='text/plain')


if __name__ == '__main__':
    print('auCombiner ready!')
    serve(app,
          host='0.0.0.0',
          port=8080)
