import boto3


def create_s3_bucket(bucket_name, Access_key, Secret_key):
    s3 = boto3.client(service_name='s3',
                      aws_access_key_id=Access_key,
                      aws_secret_access_key=Secret_key)
    s3.create_bucket(Bucket=bucket_name)
    response = s3.list_buckets()
    for b in response['Buckets']:
        return 'Existing buckets:' + b["Name"]


def upload_data(df, path):
    data = df.write \
        .format('parquet') \
        .option('header', 'true') \
        .save(path, mode='overwrite')
    return data
