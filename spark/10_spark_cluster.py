
import findspark
findspark.init()

import pyspark  
from pyspark.sql import SparkSession
import os
import pyspark.sql.functions as F
import json
import argparse


parser = argparse.ArgumentParser()
parser.add_argument('--input_green', required=False, default="*/*")
parser.add_argument('--input_yellow', required=False, default="*/*")
parser.add_argument('--output', required=False, default='tripsdata')

args = parser.parse_args()

def main(args):
    input_green = args.input_green
    input_yellow = args.input_yellow
    output = args.output

    credentials = json.load(open("../credentials/credentials.json"))
    spark = SparkSession\
        .builder\
        .master("spark://localhost:7077")\
        .appName('HelloWorld')\
        .config("spark.driver.memory", "6G") \
        .config("spark.executor.memory", "6G") \
        .config("spark.driver.maxResultSize", "6G") \
        .config("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")\
        .config(f'fs.azure.account.key.{credentials["storage_account_name"]}.blob.core.windows.net',credentials["storage_account_key"])\
        .getOrCreate()

    df_yellow = spark.read.parquet(f"../resources/datasets/yellow/{input_green}")
    df_green = spark.read.parquet(f"../resources/datasets/green/{input_yellow}")

    df_green = df_green\
        .withColumnRenamed("lpep_pickup_datetime", "pickup_datetime") \
        .withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime")

    df_yellow = df_yellow\
        .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime") \
        .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")

    common_columns = set(df_yellow.columns) & set(df_green.columns) # intersection of columns using & operator

    df_yellow = df_yellow.select(*common_columns).withColumn("service_type", F.lit("yellow"))
    df_green = df_green.select(*common_columns).withColumn("service_type", F.lit("green"))

    df_trips_data = df_green.unionAll(df_yellow)

    df_trips_data.show(5)

    df_trips_data.write.option("header", "true").parquet(f"wasbs://zoomcampcontainer@{credentials['storage_account_name']}.blob.core.windows.net/{output}")

if __name__ == '__main__':
    main(args)
