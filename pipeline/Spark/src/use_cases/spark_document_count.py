import spark_consumer as sp_con
from pyspark.sql.functions import col, from_json


# Class that counts the number of documents in a given Kafka topic and returns
# the dataframe
class countDocs:
    def __init__() -> None:
        pass

    def __new__(self, SPARK_INSTANCE, base_df, topic):
        # setting the payload schema
        valueSchema = sp_con.PayloadType(topic)
        id_filter = "account_id" if ("transactions" in topic.split(".")) else "_id"
        # Selecting the value from key-value pair based on the defined schema
        df = (
            base_df.select(from_json(col("value"), valueSchema).alias("sample"))
            .select("sample.fullDocument.*")
            .filter(col(id_filter) != "null")
            .createOrReplaceTempView("updates")  # assigning a view to it
        )
        # Count the number of documents and assign it to a dataframe
        df = SPARK_INSTANCE.sql("select COUNT(*) as COUNT from updates")
        return df
