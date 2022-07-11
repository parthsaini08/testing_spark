import os

import spark_config as sp_config


class KafkaProducer:
    def produce(self, df, topic, mode, operation, processingTime="0 seconds"):
        config = sp_config.configure()
        path = "./tmp/" + operation

        # Create the directory for checkpoint storage
        # for each feature operation separately
        try:
            os.mkdir(path)
        except OSError as error:
            print(error)
        query = (
            df
            .writeStream.outputMode(mode)
            .format("console")
            # .trigger(processingTime=processingTime)
            # .option("kafka.bootstrap.servers", config["bootstrap.servers"])
            # .option("kafka.ssl.endpoint.identification.algorithm", "https")
            # .option("kafka.sasl.mechanism", config["sasl.mechanisms"])
            # .option("kafka.security.protocol", config["security.protocol"])
            # .option("topic", topic)
            # .option("checkpointLocation", path)
            # .option(
            #     "kafka.sasl.jaas.config",
            #     "org.apache.kafka.common.security.plain.PlainLoginModule "
            #     + "required username='{api_key}' password='{api_secret}';".format(
            #         api_key=config["sasl.username"], api_secret=config["sasl.password"]
            #     ),
            # )
            .start()
        )
        try:
            query.awaitTermination()
        except Exception as E:
            print(E)
            print(
                "ERROR: Failed to connect to Kafka. \
                    Please check your credentials and try again!"
            )
