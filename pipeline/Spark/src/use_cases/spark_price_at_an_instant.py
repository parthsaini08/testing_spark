import spark_consumer as sp_con
from pyspark.sql.functions import col, from_json

# Class that gives the price at an instant
class PriceAtAnInstant:
    def __init__() -> None:
        pass

    def __new__(self, SPARK_INSTANCE, base_df, topic):
        valueSchema = sp_con.PayloadType(topic)
        # Selecting the symbol,price and cluster time and sending them to spark
        value_df = (
            base_df.select(from_json(col("value"), valueSchema).alias("sample"))
            .filter(col("sample.fullDocument.account_id") != "null")
            .select(
                "sample.fullDocument.symbol",
                "sample.fullDocument.price",
                "sample.clusterTime",
            )
        )
        return value_df
