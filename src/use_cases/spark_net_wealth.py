import spark_consumer as sp_con
from pyspark.sql.functions import col, from_json, sum, when

# Class that sends account_id and its net wealth


class NetWealth:
    def __init__() -> None:
        pass

    def __new__(self, SPARK_INSTANCE, base_df, topic):
        # setting the payload schema
        valueSchema = sp_con.PayloadType(topic)
        # Selecting account_id, transaction_code and transaction total
        df = (
            base_df.select(from_json(col("value"), valueSchema).alias("sample"))
            .filter(col("sample.fullDocument.account_id") != "null")
            .select(
                "sample.fullDocument.account_id",
                "sample.fullDocument.transaction_code",
                "sample.fullDocument.total",
            )
        )
        # changing total amount to debit for buy transaction
        df = df.withColumn(
            "total",
            when((col("transaction_code") == "buy"), col("total") * -1).otherwise(
                col("total")
            ),
        )
        # aggregating total for an account_id
        df = df.groupBy("account_id").agg(sum("total").alias("total"))

        return df
