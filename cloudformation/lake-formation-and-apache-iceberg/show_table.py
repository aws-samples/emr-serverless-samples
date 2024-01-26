import sys

from pyspark.sql import SparkSession

if __name__ == "__main__":
    table_name = sys.argv[1]
    print(f"Showing data for table {table_name}...")

    spark = SparkSession.builder.appName("Test read").getOrCreate()

    print("Listing databases...")
    spark.sql("show databases").show()

    print("Listing tables...")
    spark.sql(f"SHOW TABLES IN {table_name.split('.')[0]}").show()

    print(f"Reading table: {table_name}...")
    df = spark.read.table(table_name)

    print("Table schema:")
    df.printSchema()

    print(f"Total rows: {df.count()}")

    print("Table sample:")
    df.show(n=100, truncate=False)
