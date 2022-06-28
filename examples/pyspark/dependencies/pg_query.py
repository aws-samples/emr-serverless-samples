from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = (
    spark.read.format("jdbc")
    .option(
        "url", "jdbc:postgresql://<HOSTNAME>:5432/postgres"
    )
    .option("driver", "org.postgresql.Driver")
    .option("dbtable", "users")
    .option("user", "<USERNAME>")
    .option("password", "<PASSWORD>")
    .load()
)

df.show()
print(df.count())