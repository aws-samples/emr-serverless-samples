import sys
import uuid

from delta import DeltaTable, configure_spark_with_delta_pip
from pyspark.sql import SparkSession

builder = (
    SparkSession.builder.appName("DeltaExample")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

bucket_name = sys.argv[1]

url = f"s3://{bucket_name}/tmp/delta-lake/output/1.0.1/{uuid.uuid4()}/"

# creates a Delta table and outputs to target S3 bucket
spark.range(0, 5).write.format("delta").save(url)

if DeltaTable.isDeltaTable(spark, url):
    print("Itsa Delta!")
