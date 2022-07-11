import argparse

import findspark
import operationDictionary
from pyspark.sql import SparkSession
from useCase_test import test_spark


def callOperation(operation_string):
    # mapping the operation to the functions and the topic names
    operation_dict = operationDictionary.operation_dictionary
    # Checking if the operation is defined or not
    # if defined, set the input and output topics,
    # and the output mode and call the mapped function.
    if operation_string in operation_dict:
        operation_info = operation_dict[operation_string]
        KAFKA_INPUT_TOPIC_NAME_CONS = "mongodb.oltp." + operation_info["input_topic"]

        # Instantiating a sparkConsumer object with above parameters
        base_df = spark.read.text(
            "test_data/{}.txt".format(operation_info["input_topic"])
        )

        df = operation_info["function"](spark, base_df, KAFKA_INPUT_TOPIC_NAME_CONS)
        # Calling the testing function
        test_spark().test_spark(spark, df, operation_string)
    else:
        # Else throw an error and exit
        print(
            """ARGUMENT ERROR: Invalid operation.
            Run --help command to see supported operations."""
        )
        exit(0)


if __name__ == "__main__":
    findspark.init()
    app_name = "PythonStreaming"
    parser = argparse.ArgumentParser()
    # adding arguments
    parser.add_argument(
        "-t",
        "--test",
        help="Pass the operation to be "
        + "done as a STRING. Currently supported operations: "
        + "1.customers_count  2.transactions_count  3.accounts_count "
        + "4.price_at_an_instant  5.product_relative_reach  6.net_wealth "
        + "7.trending_list_of_products  8.trending_list_of_symbols "
        + "9.age_demographic  10.transaction_stats  11.high_risk_accounts ",
    )

    # Parsing arguments
    args = parser.parse_args()
    # getting the parameters
    test_operation = args.test
    # Initializing the Spark Instance
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("PySpark-unit-test")
        .config("spark.port.maxRetries", 30)
        .getOrCreate()
    )
    # Passing the operation to the callOperation function
    callOperation(test_operation)
