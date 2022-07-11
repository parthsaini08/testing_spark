# Importing required libraries

import spark_config as sp_config
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# Kafka initialization class
class sparkConsumer:
    def __init__() -> None:
        pass

    # New instance of consumer takes the instance and topic name,
    # and returns the base dataframe
    def __new__(self, SPARK_INSTANCE, KAFKA_INPUT_TOPIC_NAME_CONS, startingOffset):
        # Calling the configure class
        config = sp_config.configure()
        # Creating the Structured Streaming instance to read streaming data with
        # data from the config file.
        base_df = (
            SPARK_INSTANCE.readStream.format("kafka")
            .option("startingOffsets", startingOffset)
            .option("kafka.ssl.endpoint.identification.algorithm", "https")
            .option("kafka.bootstrap.servers", config["bootstrap.servers"])
            .option("subscribe", KAFKA_INPUT_TOPIC_NAME_CONS)
            .option("failOnDataLoss", "false")
            .option("kafka.security.protocol", config["security.protocol"])
            .option("kafka.sasl.mechanism", config["sasl.mechanisms"])
            .option(
                "kafka.sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule "
                + "required username='{api_key}' password='{api_secret}';".format(
                    api_key=config["sasl.username"], api_secret=config["sasl.password"]
                ),
            )
            .load()
            .selectExpr("CAST(value AS STRING)")  # Casting the value field as STRING
        )
        return base_df


# Document type of the incoming stream
class FullDocumentType:
    def __init__(self) -> None:
        pass

    # New instance takes the type variable and returns the StructType accordingly
    def __new__(self, topic) -> StructType:
        # schema for the customers documents
        customers_schema = StructType(
            [
                StructField("_id", StringType(), True),
                StructField("username", StringType(), True),
                StructField("name", StringType(), True),
                StructField("address", StringType(), True),
                StructField("email", StringType(), True),
                StructField("active", BooleanType(), False),
                StructField("accounts", ArrayType(elementType=StringType()), True),
                StructField("birthdate", TimestampType(), True),
                StructField("tier_and_details", StringType(), True),
            ]
        )
        # schema for the transactions documents
        transactions_schema = StructType(
            [
                StructField("account_id", StringType(), True),
                StructField("date", StringType(), True),
                StructField("amount", IntegerType(), True),
                StructField("transaction_code", StringType(), True),
                StructField("symbol", StringType(), True),
                StructField("price", DoubleType(), True),
                StructField("total", DoubleType(), True),
            ]
        )
        # schema for the accounts documents
        accounts_schema = StructType(
            [
                StructField("_id", StringType(), True),
                StructField("account_id", StringType(), True),
                StructField("limit", IntegerType(), True),
                StructField("products", ArrayType(elementType=StringType()), True),
            ]
        )
        # getting only the topic keyword from entire string

        type = topic.split(".")[2]
        # Defining a dict that maps String (type) to StructType
        type_dict = {
            "customers": customers_schema,
            "transactions": transactions_schema,
            "accounts": accounts_schema,
        }
        return type_dict[type]


# Payload type of the incoming stream
class PayloadType:
    def __init__(self) -> None:
        pass

    # New instance provides the StructType of the data written by
    # MongoDB to the concerned Kafka Topic
    def __new__(self, topic) -> StructType:
        # schema for the payload documents
        return StructType(
            [
                StructField("_id", StringType(), True),
                StructField("clusterTime", StringType(), True),
                StructField("documentKey", StringType(), True),
                StructField("fullDocument", FullDocumentType(topic), False),
                StructField("ns", StringType(), False),
                StructField("operationType", StringType(), True),
            ]
        )


# Data type to store data from transaction_stats topic
class StatType:
    def __init__(self) -> None:
        pass

    def __new__(self) -> StructType:
        return StructType(
            [
                StructField("account_id", StringType(), True),
                StructField("transactions_count", IntegerType(), True),
                StructField("mean_total", DoubleType(), True),
                StructField("stddev_total", DoubleType(), True),
                StructField("last_updated", TimestampType(), True),
            ]
        )
