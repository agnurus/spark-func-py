
from pyspark.sql import SparkSession

def create_spark_session(app_name="MySparkApp"):
    """
    Create and configure a Spark session.
    """
    spark = SparkSession.builder \
        .appName(app_name).master("local[*]") \
        .getOrCreate()

    # Add any additional configurations if needed
    # spark.conf.set("config_key", "config_value")

    return spark
