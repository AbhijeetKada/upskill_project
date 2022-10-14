from pyspark.sql import SparkSession


def create_dataframe(spark):
    df = spark.read. \
        format("jdbc"). \
        option("url", "jdbc:mysql://localhost:3306/upskill_sales"). \
        option("driver", "com.mysql.jdbc.Driver"). \
        option("dbtable", "sales"). \
        option("user", "root"). \
        option("password", "sangola@123").load()
    df.show()


if __name__ == '__main__':
    sparks = SparkSession.builder.config("spark.jars", "mysql-connector-java-8.0.22.jar"). \
        master("local"). \
        appName("main"). \
        getOrCreate()
    create_dataframe(sparks)
