"""
This is an example of how to read data from S3 using Spark.
"""

from pyspark.sql import SparkSession

import sys

# parsing first argument to get the input bucket name and path (e.g. s3://my-bucket/my-path)
input_path = sys.argv[1]

spark = SparkSession.builder.appName("ReaderExample").enableHiveSupport().getOrCreate()

df = spark.read.parquet(input_path)

df.printSchema()
df.show(20, False)

