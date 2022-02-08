import sys
from pyspark.sql import SparkSession


def add_venv_to_path(archive_name):
    print(f"Adding {archive_name} to sys.path")
    userFilesDir = [match for match in sys.path if "userFiles" in match][0]
    sys.path.append(f"{userFilesDir}/{archive_name}/lib/python3.7/site-packages")
    return f"{userFilesDir}/{archive_name}"


if __name__ == "__main__":
    """
    Usage: glow <venv_archive.tar.gz>
    """
    if len(sys.argv) != 2:
        print("Invalid arguments, please supply <venv_archive.tar.gz>")
        sys.exit(1)

    spark = SparkSession.builder.appName("GlowPrep").getOrCreate()

    archive_name = sys.argv[1]
    added_path = add_venv_to_path(archive_name)

    # Imports need to be made after adding the virtualenv to the path
    import glow

    # Read some vcf data
    vcf_file = "s3://1000genomes/phase1/analysis_results/integrated_call_sets/ALL.chr17.integrated_phase1_v3.20101123.snps_indels_svs.genotypes.vcf.gz"
    spark = glow.register(spark)
    df = spark.read.format("vcf").load(vcf_file)
    df.show()
