from pyspark.sql import SparkSession


def create_spark_session():
    spark = SparkSession.builder.master("local[*]")\
        .appName("iToll")\
            .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.1,"
        "com.clickhouse:clickhouse-jdbc:0.3.2-patch11,"
        "mysql:mysql-connector-java:8.0.22"
    ).getOrCreate()
    return spark


def extract_data_from_mysql(spark_session, database_host, database_name, query, username, password):
    df = spark_session.read.format("jdbc") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("url", "jdbc:mysql://{}:3306/{}").format(database_host, database_name) \
        .option("query", query) \
        .option("user", username) \
        .option("password", password) \
        .load()
    return df