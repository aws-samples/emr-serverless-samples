from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import sys

kinesis_stream_name = sys.argv[1]
s3_checkpoint_bucket = sys.argv[2]


spark = SparkSession \
            .builder \
                .appName("StructuredStreaming") \
                    .getOrCreate()

kinesis = spark.readStream \
.format("aws-kinesis") \
.option("kinesis.region", "us-west-2") \
.option("kinesis.streamName", kinesis_stream_name) \
.option("kinesis.consumerType", "GetRecords") \
.option("kinesis.endpointUrl", "https://kinesis.us-west-2.amazonaws.com") \
.option("kinesis.startingposition", "LATEST") \
.load()


pythonSchema = StructType() \
.add("transactionId", StringType()) \
.add("amount", FloatType()) \
.add("transactionTime", TimestampType()) \
.add("cardHolderId", StringType())


events= kinesis \
  .selectExpr("cast (data as STRING) jsonData") \
  .select(from_json("jsonData", pythonSchema).alias("events")) \
  .select("events.*")


highVolumeTransactionsDF = events \
    .groupBy(window("transactionTime", "2 minutes"), "cardHolderId") \
    .count() \
    .filter("count >= 5") \
    .selectExpr("window.start", "window.end", "cardHolderId", "count")



highVolumeTransactionsDF.writeStream.format("console").outputMode("update").trigger(processingTime='10 seconds').option("checkpointLocation", s3_checkpoint_bucket).start().awaitTermination()

