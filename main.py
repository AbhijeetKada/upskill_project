from pyspark.sql import SparkSession
import configparser
from ingestion import upload_data, create_s3_bucket

# Reading configs
config = configparser.ConfigParser()
config.read("../pythonProject/config.ini")


def create_dataframe(spark):
    df = spark.read. \
        format("jdbc"). \
        option("url", config.get('mysql', 'url')). \
        option("driver", config.get('mysql', 'driver')). \
        option("dbtable", config.get('mysql', 'tablename')). \
        option("user", config.get('mysql', 'user')). \
        option("password", config.get('mysql', 'password')).load()
    return df


if __name__ == '__main__':
    spark = SparkSession \
        .builder.config("spark.jars",
                        'C:/Users/Apurva Waghmode/PycharmProjects/pythonProject/Jar_files/aws-java-sdk-bundle-1.11.563.jar,C:/Users/Apurva Waghmode/PycharmProjects/pythonProject/Jar_files/hadoop-aws-3.2.2.jar,C:/Users/Apurva Waghmode/PycharmProjects/pythonProject/Jar_files/mysql-connector-java-8.0.22.jar') \
        .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
        .config('spark.hadoop.fs.s3a.access.key', config.get('aws', 'AWS_ACCESSKEY')) \
        .config('spark.hadoop.fs.s3a.secret.key', config.get('aws', 'AWS_SECRETKEY')) \
        .master("local") \
        .getOrCreate()

    a = create_dataframe(spark)
    path = "s3a://aws-s3-bucket-123/"
    create_s3_bucket(config.get('aws', 'AWS_BUCKETNAME'), config.get('aws', 'AWS_ACCESSKEY'),config.get('aws', 'AWS_SECRETKEY'))
    upload_data(a, path)
