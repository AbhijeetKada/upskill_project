from pyspark.shell import sc
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import StructType, StructField, StringType
from src.main.mysql_reader import get_diff_in_days,filter_data


def test_get_diff_in():
    days =get_diff_in_days("2022-10-01 01:00:00","2022-10-31 01:00:00")
    assert days ==30


def test_filter():
    spark = SparkSession \
             .builder.config("spark.jars",
                             r"C:\Users\Abhijeet Kadam\projects\Sales\jar files\mysql-connector-java-8.0.22.jar") \
             .master("local") \
             .getOrCreate()
    data = spark.read.csv(r"C:\Users\Abhijeet Kadam\projects\UpSkill_sales_project-main\data\sales_data.csv",
                                                    header=True, sep=";")
    data.select(to_timestamp('UpdatedAt', 'yyyy-mm-dd HH:mm:ss'))
    data.createOrReplaceTempView('sales_data')
    da = filter_data(spark, 29, "sales_data", "UpdatedAt", "2022-10-01 01:00:00", "2022-11-11 01:00:00")

    require = [("Australia and Oceania",	"Palau",	"Office Supplies",	"Online",	"H",	"3/6/2016",	"517073523",	"2022-11-09",	"2401",	"651.21",	"524.96",	"1563560.0",	"1260430.0",	"303126.0",	"2022-11-09 09:58:33",	"2022-11-01 13:02:21"),
                   ("Europe",	"Poland",	"Beverages",	"Online",	"L",	"4/18/2010",	"380507028",	"5/26/2010",	"9340",	"47.45",	"31.79","443183.0",	"296919.0",	"146264.0",	"2022-11-01 13:02:21",	"2022-11-01 13:02:21"),
                   ("Asia", "US", "Office Supplies", "online", "H", "01-11-2022", "380507028", "02-10-2022", "3232", "47.45", "12.0", "324657.0", "32543.0", "34566.0", "2022-11-02 09:56:33", "2022-11-01 13:06:39"),
                   ("Asia", "US", "Office Supplies", "online", "H", "01-11-2022", "380507028", "02-10-2022", "3232", "47.45", "12.0", "324657.0", "32543.0", "34566.0", "2022-11-02 09:56:33", "2022-11-01 13:07:46"),
                   ("Asia", "US", "Office Supplies", "online", "H", "01-11-2022", "380507028", "02-10-2022", "3232", "47.45", "12.0", "324657.0", "32543.0", "34566.0", "2022-11-02 09:56:33", "2022-11-01 13:10:05"),
                   ("AUS",	"NZ",	"Supplies",	"offline",	"L",	"01-08-2022",	"380507421",	"02-05-2021",	"3232",	"47.45",	"12.0",	"324657.0",	"32543.0",	"34566.0",	"2022-11-01 15:39:35",	"2022-11-01 15:39:35"),
                   ("Middle",	"CA",	"Supplies",	"online",	"L",	"01-08-2022",	"380507421",	"2022-11-04",	"3232",	"45.45",	"22.0",	"367657.0",	"39843.0",	"24566.0",	"2022-11-04 11:57:32",	"2022-11-04 11:12:46"),
                   ("Asia",	"INDIA",	"Office Supplies",	"online",	"H",	"01-11-2022",	"380507028",	"02-10-2022",	"3232",	"47.45",	"12.0",	"324657.0",	"32543.0",	"34566.0",	"2022-11-09 09:50:35",	"2022-11-09 09:50:35"),
                   ("US",	"NY",	"supplies",	"offline",	"L",	"11-12-2021",	"24556",	"23-12-2021",	"45656",	"47.45",	"45.0",	"324657.0",	"35556.0",	"1323.0",	"2022-11-09 09:52:41",	"2022-11-09 09:52:41"),
                   ("US",	"BZ",	"supplies",	"online",	"H",	"23-02-2021",	"34566",	"23-12-2021",	"76544",	"37.45",	"23.0",	"754534.0",	"76556.0",	"765432.0",	"2022-11-09 09:57:26",	"2022-11-09 09:57:26"),
                   ("Middle",	"Dubai",	"office supplies",	"offline",	"H",	"21-12-2021",	"53444",	"12-11-2022",	"455666",	"37.45",	"54.0",	"45556.0",	"234545.0",	"566432.0",	"2022-11-09 09:57:26",	"2022-11-09 09:57:26")]

    myrdd = sc.parallelize(require)
    schema = StructType(
            [
                StructField('Region', StringType(), True),
                StructField('Country', StringType(), True),
                StructField('Item Type', StringType(), True),
                StructField('Sales Channel', StringType(), True),
                StructField('Order Priority', StringType(), True),
                StructField('Order Date', StringType(), True),
                StructField('Order ID', StringType(), True),
                StructField('ShipDate', StringType(), True),
                StructField('Units Sold', StringType(), True),
                StructField('Unit Price', StringType(), True),
                StructField('Unit Cost', StringType(), True),
                StructField('Total Revenue', StringType(), True),
                StructField('Total Cost', StringType(), True),
                StructField('Total Profit', StringType(), True),
                StructField('UpdatedAt', StringType(), True),
                StructField('CreatedAt', StringType(), True)
            ])

    required = spark.createDataFrame(data=myrdd, schema=schema)
    assert required.collect() == da.collect()





