# MIT No Attribution

# Copyright 2021 Amazon.com, Inc. or its affiliates

# Permission is hereby granted, free of charge, to any person obtaining a copy of this
# software and associated documentation files (the "Software"), to deal in the Software
# without restriction, including without limitation the rights to use, copy, modify,
# merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
# INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
# PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

import io
import sys

import boto3
import matplotlib.pyplot as plt
import pyspark.sql.functions as F
import seaborn as sns
from pyspark.sql import DataFrame, SparkSession


def read_location_data(spark: SparkSession, location_id: str, year: int) -> DataFrame:
    """
    Reads the raw data for the provided location ID and year and returns a DataFrame
    """
    df = spark.read.csv(
        f"s3://noaa-gsod-pds/{year}/{location_id}.csv", header=True, inferSchema=True
    ).withColumn("month", F.col("DATE").substr(0, 7))

    # df.groupBy("month").agg(F.round(F.avg("TEMP"), 2).alias("avg_temp")).sort("month").show()

    # Cool, now see if we can do a simple single-year plot
    return df.select("month", "temp")


def generate_sluggie(df: DataFrame) -> bytes:
    sns.set_theme(style="white", rc={"axes.facecolor": (0, 0, 0, 0)})

    # Initialize the FacetGrid object
    pal = sns.cubehelix_palette(12, rot=-0.25, light=0.7)
    g = sns.FacetGrid(
        df.toPandas(), row="month", hue="month", aspect=15, height=0.5, palette=pal
    )

    # Draw the densities in a few steps
    g.map(
        sns.kdeplot,
        "temp",
        bw_adjust=0.5,
        clip_on=False,
        fill=True,
        alpha=1,
        linewidth=1.5,
    )
    g.map(sns.kdeplot, "temp", clip_on=False, color="w", lw=2, bw_adjust=0.5)

    # passing color=None to refline() uses the hue mapping
    g.refline(y=0, linewidth=2, linestyle="-", color=None, clip_on=False)

    # Define and use a simple function to label the plot in axes coordinates
    def label(x, color, label):
        ax = plt.gca()
        ax.text(
            0,
            0.2,
            label,
            fontweight="bold",
            color=color,
            ha="left",
            va="center",
            transform=ax.transAxes,
        )

    g.map(label, "temp")

    # Set the subplots to overlap
    g.figure.subplots_adjust(hspace=-0.25)

    # Remove axes details that don't play well with overlap
    g.set_titles("")
    g.set(yticks=[], ylabel="")
    g.despine(bottom=True, left=True)

    # Write out a png
    img_data = io.BytesIO()
    g.figure.savefig(img_data, format="png")
    img_data.seek(0)
    return img_data


def upload_to_s3(f: bytes, bucket: str, key: str):
    """
    Uploads the provided bytes to the specified S3 bucket and key
    """
    # spark.sparkContext.parallelize([b]).saveAsObject(f"s3a://{bucket}/{key}")
    s3 = boto3.client("s3")
    s3.upload_fileobj(f, bucket, key)


if __name__ == "__main__":
    """
    Usage: noaa_slugplot <location_id> <year> <s3_bucket> <s3_key>

    Creates a temperature Slugplot for the provided location ID and outputs a png to the provided S3 location
    """
    spark = SparkSession.builder.appName("SlugPlot").getOrCreate()
    location_id = sys.argv[1]
    year = sys.argv[2]
    s3_bucket = sys.argv[3]
    s3_key = sys.argv[4]

    month_df = read_location_data(spark, location_id, year)
    img_data = generate_sluggie(month_df)
    upload_to_s3(img_data, s3_bucket, s3_key)
