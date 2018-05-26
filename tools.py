import os
from pyspark.sql import SparkSession

def setup_spark_config(appName="Exploring Million Song Dataset"):
    if 'ubuntu' in os.path.dirname(os.path.realpath(__file__)):
        spark = SparkSession \
        .builder \
        .master("spark://192.168.1.141:7077") \
        .appName(appName) \
        .getOrCreate()
    else:
        spark = SparkSession \
        .builder \
        .appName(appName) \
        .getOrCreate()

    sc = spark.sparkContext
    return sc, spark

def read_parquet_files(basedir, spark):
    if basedir is None:
        print("No provided directory")
        return
    elif os.path.isdir(basedir):
        print("Reading songs from parquet files to DataFrame")
        songs_df = spark.read.parquet(basedir)
        return songs_df
    else:
        print("No parquet files to read in provided directory, aborting")
        return
