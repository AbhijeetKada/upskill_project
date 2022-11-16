from configparser import ConfigParser
from datetime import datetime
from pyspark.sql import SparkSession
from upload_to_s3 import upload_to_aws


def data_read(spark, TABLE_NAME, DB_DRIVER, DB_USER, DB_PASS, URL):
    data_frame = spark.read. \
        format("jdbc"). \
        option("url", f'{URL}'). \
        option("driver", f'{DB_DRIVER}'). \
        option("dbtable", f'{TABLE_NAME}'). \
        option("user", f'{DB_USER}'). \
        option("password", f'{DB_PASS}'). \
        load()

    data_frame.createOrReplaceTempView(f'{TABLE_NAME}')
    return f'{TABLE_NAME}'
    return data_frame


def get_diff_in_days(start_date, end_date):
    date_format_str = '%Y-%m-%d %H:%M:%S'
    start = datetime.strptime(f'{start_date}', date_format_str)
    end = datetime.strptime(f'{end_date}', date_format_str)
    dd = end - start
    return dd.days


def filter_data(spark, diff_in_days, TABLE_NAME, wdata, start_date, end_date):
    if diff_in_days > 30:
        print("Days are high")
        quit()
    else:
        dat = spark.sql(f'select * from {TABLE_NAME} where {wdata}>="{start_date}" and {wdata}<="{end_date}"')
        return dat


def write_to_file(dat):
    today = datetime.now()
    d_date = today.strftime("%d_%m_%Y")
    file_name = "sales_{}.csv".format(d_date)
    dat.write.format('csv').option('header', 'true').save(f"..\\upskill_project\\output\\{file_name}",
                                                          mode='overwrite')


def upload(dat):
    upload_to_aws(dat)


if __name__ == '__main__':
    file = 'config files/config.ini'
    config = ConfigParser()
    config.read(file)
    driver_name = config['database']['DB_DRIVER']
    db_user = config['database']['DB_USER']
    db_pass = config['database']['DB_PASS']
    db_url = config['database']['URL']
    tables = config['database']['tables']
    access_key = config['AWS']['ACCESS_KEY']
    secret_key = config['AWS']['SECRET_KEY']

    with open('config files/config.ini', 'r') as f:
        config.read(f)
    spark = SparkSession \
        .builder.config("spark.jars",
                        r"C:\Users\Abhijeet Kadam\projects\upskill_project\jar files\mysql-connector-java-8.0.22.jar,C:\Users\Abhijeet Kadam\projects\upskill_project\jar files\aws-java-sdk-bundle-1.11.563.jar,C:\Users\Abhijeet Kadam\projects\upskill_project\jar files\hadoop-aws-3.2.2.jar") \
        .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
        .config('spark.hadoop.fs.s3a.access.key', access_key) \
        .config('spark.hadoop.fs.s3a.secret.key', secret_key) \
        .master("local") \
        .getOrCreate()

    date_format = '%Y-%m-%d %H:%M:%S'
    start_date = "2022-11-05 01:00:00"
    end_date = "2022-11-11 01:00:00"

    TABLE_NAME = "sales_data"
    wdata = "UpdatedAt"

    tables_nm = data_read(spark, TABLE_NAME, driver_name, db_user, db_pass, db_url)
    diff_in_days = get_diff_in_days(start_date, end_date)
    data = filter_data(spark, diff_in_days, tables_nm, wdata, start_date, end_date)
    write_to_file(data)
    upload(data)
