from configparser import ConfigParser
import boto3
from botocore.exceptions import NoCredentialsError

file = r'..\projects\upskill_project\config files\config.ini'
config = ConfigParser()
config.read(file)
with open(file, 'r') as f:
    config.read(f)
ACCESS_KEY = config['AWS']['ACCESS_KEY']
SECRET_KEY = config['AWS']['SECRET_KEY']


def upload_to_aws(df):
    s3 = boto3.client(
        service_name='s3',
        region_name='us-east-1',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY
    )

    try:
        data = df.write \
            .format('csv') \
            .option('header', 'true') \
            .save('s3a://rawdata-123/', mode='append')
        print("Upload Successful")
        return True
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False
