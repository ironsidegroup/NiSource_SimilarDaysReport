import boto3
from openpyxl import load_workbook
from tempfile import NamedTemporaryFile
import boto3
import botocore
import io
import json
import pandas as pd

s3_resource = boto3.resource('s3')
s3 = boto3.client('s3')


def write_json(file, bucket, key):
    s3_resource.Object(bucket, key).put(Body=json.dumps(file))
    return "s3://{}/{}".format(bucket, key)


def write_csv(df, bucket, key):
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False, header=True)
    s3_resource.Object(bucket, key).put(Body=csv_buffer.getvalue())
    return "s3://{}/{}".format(bucket, key)


def read_csv(bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(io.BytesIO(obj['Body'].read()))

def download_file(bucket, key):
    try:
        file = NamedTemporaryFile(suffix = '.xlsx', delete=False)
        s3.Bucket(bucket).download_file(key, file.name)
        return file.name
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            return None
        else:
            raise
    else:
        raise

def upload_workbook(workbook, bucket, key):
    with NamedTemporaryFile() as tmp:
        workbook.save(tmp.name)
        tmp.seek(0)
        s3.meta.client.upload_file(tmp.name, bucket, key)
