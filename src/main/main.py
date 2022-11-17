import configparser
from spark_session import get_spark_session
from reader import create_dataframe
from ingestion import upload_raw_data, create_s3_bucket


# Reading configs
config = configparser.ConfigParser()
config.read(r"../PycharmProjects/upskill_project/config_file/config.ini")

if __name__ == '__main__':
    spark = get_spark_session()
    a = create_dataframe(spark)
    create_s3_bucket(config.get('aws', 'bucket_name'), config.get('aws', 'AWS_ACCESSKEY'),
                     config.get('aws', 'AWS_SECRETKEY'))
    upload_raw_data(a, config.get('aws', 'path'))
