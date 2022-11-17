import configparser

# Reading configs
config = configparser.ConfigParser()
config.read(r"../PycharmProjects/upskill_project/config_file/config.ini")


# Create dataframe
def create_dataframe(spark):
    df = spark.read. \
        format("jdbc"). \
        option("url", config.get('mysql', 'url')). \
        option("driver", config.get('mysql', 'driver')). \
        option("dbtable", config.get('mysql', 'tablename')). \
        option("user", config.get('mysql', 'user')). \
        option("password", config.get('mysql', 'password')).load()
    return df
