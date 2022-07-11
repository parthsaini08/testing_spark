import spark_consumer as sp_con
from pyspark.sql.functions import (
    col,
    count,
    from_json,
    from_unixtime,
    max,
    mean,
    stddev,
    to_timestamp,
)


class TransactionsAggregator:
    def __init__(self) -> None:
        pass

    def __new__(self, SPARK_INSTANCE, base_df, topic):
        valueSchema = sp_con.PayloadType(topic)

        base_df = (
            base_df.withColumn("data", from_json(col("value"), valueSchema))
            .select("data.*")
            .select(
                "fullDocument.account_id",
                "fullDocument.transaction_code",
                "fullDocument.total",
                to_timestamp(from_unixtime(col("clusterTime") / 1000)).alias(
                    "clusterTime"
                ),
            )
        )

        base_df.printSchema()

        avg_df = base_df.groupBy("account_id").agg(
            count("total").alias("transactions_count"),
            mean("total").alias("mean_total"),
            stddev("total").alias("stddev_total"),
            max("clusterTime").alias("last_updated"),
        )

        return avg_df
