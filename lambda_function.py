import os
import zipfile
import json
import base64
from urllib.request import Request, urlopen
from io import BytesIO
from datetime import datetime
import csv
import boto3

def lambda_handler(event, context):
    # Get credentials from Secrets Manager
    secrets_client = boto3.client('secretsmanager')
    secret = secrets_client.get_secret_value(SecretId='billboard-kaggle-creds')
    secret_dict = json.loads(secret['SecretString'])
    kaggle_username = secret_dict['KAGGLE_USERNAME']
    kaggle_key = secret_dict['KAGGLE_KEY']
    bucket = secret_dict['S3_BUCKET']

    s3 = boto3.client('s3')

    # Basic Auth for Kaggle API
    auth = base64.b64encode(f"{kaggle_username}:{kaggle_key}".encode()).decode()
    headers = {'Authorization': f'Basic {auth}'}

    # Check for new version via metadata
    metadata_url = 'https://www.kaggle.com/api/v1/datasets/view/ludmin/billboard'
    req = Request(metadata_url, headers=headers)
    with urlopen(req) as response:
        metadata = json.loads(response.read().decode())
    last_update = metadata['lastUpdated']

    metadata_key = 'bronze/metadata.json'
    try:
        prev_metadata_obj = s3.get_object(Bucket=bucket, Key=metadata_key)
        prev_metadata = json.loads(prev_metadata_obj['Body'].read().decode('utf-8'))
        prev_update = prev_metadata['last_update']
    except s3.exceptions.NoSuchKey:
        prev_update = None

    if prev_update and last_update <= prev_update:
        return {'statusCode': 200, 'body': 'No new update available.'}

    # Download dataset ZIP
    download_url = 'https://www.kaggle.com/api/v1/datasets/download/ludmin/billboard'
    req = Request(download_url, headers=headers)
    with urlopen(req) as response:
        zip_data = BytesIO(response.read())

    # Extract hot100.csv from ZIP
    with zipfile.ZipFile(zip_data) as zip_ref:
        zip_ref.extract('hot100.csv', '/tmp')

    # Filter delta with streaming
    with open('/tmp/hot100.csv', 'r') as f:
        reader = csv.reader(f)
        header = next(reader)  # Skip header
        new_rows = []
        for row in reader:
            if row:  # Skip empty rows
                new_rows.append(row)

    new_dates = [datetime.strptime(row[0], '%Y-%m-%d') for row in new_rows if row]

    response = s3.list_objects_v2(Bucket=bucket, Prefix='bronze/hot100_')
    if 'Contents' in response and len(response['Contents']) > 0:
        latest_key = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)[0]['Key']
        s3.download_file(bucket, latest_key, '/tmp/previous.csv')
        with open('/tmp/previous.csv', 'r') as f:
            reader = csv.reader(f)
            header = next(reader)
            prev_rows = [row for row in reader if row]
        max_prev_date = max(datetime.strptime(row[0], '%Y-%m-%d') for row in prev_rows if row)
        delta_rows = [row for row in new_rows if datetime.strptime(row[0], '%Y-%m-%d') > max_prev_date]
    else:
        delta_rows = new_rows

    if not delta_rows:
        return {'statusCode': 200, 'body': 'No new data.'}

    # Upload delta
    timestamp = datetime.now().strftime('%Y-%m-%d')
    delta_path = f'/tmp/hot100_delta_{timestamp}.csv'
    with open(delta_path, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(delta_rows)
    s3.upload_file(delta_path, bucket, f'bronze/hot100_delta_{timestamp}.csv')

    # Update metadata
    new_metadata = {'last_update': last_update, 'max_date': max_prev_date.strftime('%Y-%m-%d') if 'max_prev_date' in locals() else new_dates[-1].strftime('%Y-%m-%d')}
    s3.put_object(Bucket=bucket, Key=metadata_key, Body=json.dumps(new_metadata))

    # Cleanup
    for file in ['/tmp/hot100.csv', delta_path, '/tmp/dataset-metadata.json']:
        if os.path.exists(file):
            os.remove(file)

    return {'statusCode': 200, 'body': f'Uploaded hot100_delta_{timestamp}.csv to S3!'}
