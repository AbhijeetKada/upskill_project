from pyspark.sql import SparkSession
import configparser

# Reading configs
config = configparser.ConfigParser()
config.read(r"../PycharmProjects/upskill_project/config_file/config.ini")


# Create Spark session
def get_spark_session():
    spark = SparkSession \
        .builder.config("spark.jars",
                        '../PycharmProjects/pythonProject/Jar_files/aws-java-sdk-bundle-1.11.563.jar,'
                        '../PycharmProjects/pythonProject/Jar_files/hadoop-aws-3.2.2.jar,'
                        '../PycharmProjects/pythonProject/Jar_files/mysql-connector-java-8.0.22.jar') \
        .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
        .config('spark.hadoop.fs.s3a.access.key', config.get('aws', 'AWS_ACCESSKEY')) \
        .config('spark.hadoop.fs.s3a.secret.key', config.get('aws', 'AWS_SECRETKEY')) \
        .master("local") \
        .getOrCreate()
    return spark
