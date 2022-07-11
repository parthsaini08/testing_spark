# Importing required libraries
import os
from configparser import ConfigParser

from pyspark.sql import SparkSession


# Configure class to read config file and pass the values
class configure:
    def __init__() -> None:
        pass

    def __new__(self):
        # Initializing the argument parse
        config_parser = ConfigParser()
        config_parser.read("./config.ini")
        config = dict(config_parser["default"])
        return config


# Spark initialization class
class sparkInitializer:
    def __init__() -> None:
        pass

    # New instance takes the kafka and spark version
    # and the appName, and returns the spark instance
    def __new__(self, appName, kafka_ver, spark_ver):
        # Included the OS environment for ease of running on the CLI
        os.environ["PYSPARK_SUBMIT_ARGS"] = (
            "--packages org.apache.spark:spark-streaming-kafka-0-10_"
            + kafka_ver
            + ":"
            + spark_ver
            + ",org.apache.spark:spark-sql-kafka-0-10_"
            + kafka_ver
            + ":"
            + spark_ver
            + " pyspark-shell"
        )
        try:
            # Creating the spark session
            SPARK_INSTANCE = SparkSession.builder.appName(appName).getOrCreate()
            SPARK_CONTEXT = SPARK_INSTANCE.sparkContext
        except Exception as E:
            print(E)
            print(
                "ERROR: Spark or Kafka versions are wrong.\
                     Check your versions and try again!"
            )
        # returning spark instance and context
        return SPARK_CONTEXT, SPARK_INSTANCE
