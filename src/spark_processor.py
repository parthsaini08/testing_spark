import argparse
import os

import dictionary as dict
import findspark
import spark_config as sp_config
import spark_consumer as sp_con
import spark_producer as sp_prod


def callOperation(operation_string):
    # mapping the operation to the functions and the topic names
    operation_dict = dict.dictionary
    # Checking if the operation is defined or not
    # if defined, set the input and output topics,
    # and the output mode and call the mapped function.
    if operation_string in operation_dict:
        operation_info = operation_dict[operation_string]
        KAFKA_INPUT_TOPIC_NAME_CONS = "mongodb.oltp." + operation_info["input_topic"]
        KAFKA_INPUT_READING_OFFSET = operation_info["input_reading_offset"]

        KAFKA_OUTPUT_TOPIC_NAME_CONS = "spark." + operation_string
        KAFKA_OUTPUT_MODE = operation_info["output_mode"]

        try:
            KAFKA_OUTPUT_PROCESSING_TIME = operation_info["producer_processing_time"]
        except KeyError:
            KAFKA_OUTPUT_PROCESSING_TIME = "0 seconds"

        # Instantiating a sparkConsumer object with above parameters
        base_df = sp_con.sparkConsumer(
            spark, KAFKA_INPUT_TOPIC_NAME_CONS, KAFKA_INPUT_READING_OFFSET
        )
        df = operation_info["function"](spark, base_df, KAFKA_INPUT_TOPIC_NAME_CONS)
    else:
        # Else throw an error and exit
        print(
            """ARGUMENT ERROR: Invalid operation.
            Run --help command to see supported operations."""
        )
        exit(0)

    # Return the dataframe produced and output topic name,
    # outputMode and producer processingTime.
    return (
        df,
        KAFKA_OUTPUT_TOPIC_NAME_CONS,
        KAFKA_OUTPUT_MODE,
        KAFKA_OUTPUT_PROCESSING_TIME,
    )


if __name__ == "__main__":

    findspark.init()
    app_name = "PythonStreaming"
    chkpnt_path = "./tmp"
    # Creating the checkpoint folder.
    try:
        os.mkdir(chkpnt_path)
    except OSError as error:
        print(error)
    # Initializing the argument parse
    parser = argparse.ArgumentParser()
    # adding arguments
    parser.add_argument(
        "-k",
        "--kafka",
        help="Pass the kafka version as a parameter. Default value : 2.12",
        default="2.12",
    )
    parser.add_argument(
        "-s",
        "--spark",
        help="Pass the spark version as a parameter. Default value : 3.2.0",
        default="3.2.0",
    )
    parser.add_argument(
        "-o",
        "--operation",
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
    kafka_version = args.kafka
    spark_version = args.spark
    operation = args.operation
    # If topic not provided, taking 'customers' by default
    # Initializing the Spark Instance
    sc, spark = sp_config.sparkInitializer(app_name, kafka_version, spark_version)
    # Passing the operation to the callOperation function
    df, output_topic, output_mode, processing_time = callOperation(operation)
    # Instantiating a Kafka producer object
    producer = sp_prod.KafkaProducer()
    # Passing the topic, mode and the dataframe as parameters.
    producer.produce(df, output_topic, output_mode, operation, processing_time)
