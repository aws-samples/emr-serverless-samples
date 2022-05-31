import sys
from pyspark.sql import SparkSession

import great_expectations as ge
from great_expectations.profile.basic_dataset_profiler import BasicDatasetProfiler
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset
from great_expectations.render.renderer import *
from great_expectations.render.view import DefaultJinjaPageView


if __name__ == "__main__":
    """
        Usage: ge-profile <s3_output_path.html>
    """
    spark = SparkSession\
        .builder\
        .appName("GEProfiler")\
        .getOrCreate()
    
    if len(sys.argv) != 2:
        print("Invalid arguments, please supply <s3_output_path.html>")
        sys.exit(1)

    output_path = sys.argv[1]

    # Read some trip data
    df = spark.read.csv("s3://nyc-tlc/trip data/yellow*.csv", header=True)
    df.show()

    # Now profile it with Great Expectations and write the results to S3
    expectation_suite, validation_result = BasicDatasetProfiler.profile(SparkDFDataset(df.limit(1000)))
    document_model = ProfilingResultsPageRenderer().render(validation_result)
    html = DefaultJinjaPageView().render(document_model)
    spark.sparkContext.parallelize([html]).coalesce(1).saveAsTextFile(output_path)
