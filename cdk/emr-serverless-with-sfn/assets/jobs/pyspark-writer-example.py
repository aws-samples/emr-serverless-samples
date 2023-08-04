"""
This is an example of how to write data to S3 using Spark.
"""

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

import sys

# parsing first argument to get the output bucket name and path (e.g. s3://my-bucket/my-path)
output_path = sys.argv[1]

spark = SparkSession.builder.appName("WriterExample").enableHiveSupport().getOrCreate()

data = spark.range(100) \
    .withColumn("random_uuid", F.expr('replace(uuid(), "-", "")')) \
    .withColumn("random_double", F.rand())

data.write.mode("overwrite").parquet(output_path)
