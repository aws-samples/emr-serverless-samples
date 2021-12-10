import sys
from datetime import date

from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType


def findLargest(df: DataFrame, col_name: str) -> Row:
    """
    Find the largest value in `col_name` column.
    Values of 99.99, 999.9 and 9999.9 are excluded because they indicate "no reading" for that attribute.
    While 99.99 _could_ be a valid value for temperature, for example, we know there are higher readings.
    """
    return (
        df.select(
            "STATION", "DATE", "LATITUDE", "LONGITUDE", "ELEVATION", "NAME", col_name
        )
        .filter(~F.col(col_name).isin([99.99, 999.9, 9999.9]))
        .orderBy(F.desc(col_name))
        .limit(1)
        .first()
    )


if __name__ == "__main__":
    """
    Usage: extreme-weather [year]

    Displays extreme weather stats (highest temperature, wind, precipitation) for the given, or latest, year.
    """
    spark = SparkSession.builder.appName("ExtremeWeather").getOrCreate()

    if len(sys.argv) > 1:
        year = sys.argv[1]
    else:
        year = date.today().year

    df = spark.read.csv(f"s3://noaa-gsod-pds/{year}/", header=True, inferSchema=True)
    print(f"The amount of weather readings in {year} is: {df.count()}\n")

    print(f"Here are some extreme weather stats for {year}:")
    stats_to_gather = [
        {"description": "Highest temperature", "column_name": "MAX", "units": "°F"},
        {
            "description": "Highest all-day average temperature",
            "column_name": "TEMP",
            "units": "°F",
        },
        {"description": "Highest wind gust", "column_name": "GUST", "units": "mph"},
        {
            "description": "Highest average wind speed",
            "column_name": "WDSP",
            "units": "mph",
        },
        {
            "description": "Highest precipitation",
            "column_name": "PRCP",
            "units": "inches",
        },
    ]

    for stat in stats_to_gather:
        max_row = findLargest(df, stat["column_name"])
        print(
            f"  {stat['description']}: {max_row[stat['column_name']]}{stat['units']} on {max_row.DATE} at {max_row.NAME} ({max_row.LATITUDE}, {max_row.LONGITUDE})"
        )
