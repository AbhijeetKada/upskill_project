import boto3
import configparser
from botocore.exceptions import NoCredentialsError

# Reading configs
config = configparser.ConfigParser()
config.read(r"../PycharmProjects/upskill_project/config_file/config.ini")


# Creating AWS S3 Bucket
def create_s3_bucket(bucket_name, Access_key, Secret_key):
    s3 = boto3.client(service_name='s3',
                      aws_access_key_id=Access_key,
                      aws_secret_access_key=Secret_key)
    s3.create_bucket(Bucket=bucket_name)


# Uploaded raw data in S3 Bucket
def upload_raw_data(df, path):
    s3 = boto3.resource(
        service_name='s3',
        region_name='us-east-1',
        aws_access_key_id=config.get('aws', 'AWS_ACCESSKEY'),
        aws_secret_access_key=config.get('aws', 'AWS_SECRETKEY'))
    try:
        for bucket in s3.buckets.all():
            print(bucket.name)
        data = df.write \
            .format('csv') \
            .option('header', 'true') \
            .save(path, mode='append')
        print("Upload Successful")
        return data
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False
