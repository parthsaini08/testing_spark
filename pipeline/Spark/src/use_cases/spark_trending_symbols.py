import spark_consumer as sp_con
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
    from_unixtime,
    substring,
    to_timestamp,
    udf,
    window,
)
from pyspark.sql.types import IntegerType, LongType

k = 0

# Class that finds the trending list of symbols


class TrendingSymbols:
    def __init__() -> None:
        pass

    def process(self):
        global k
        temp = (k % 3) + 1
        k = k + 1
        return temp

    def __new__(self, SPARK_INSTANCE, base_df, topic):
        global k
        # setting the payload schema
        valueSchema = sp_con.PayloadType(topic)
        # Selecting the value from key-value pair based on the defined schema
        value_df = (
            base_df.select(
                from_json(col("value").cast("string"), valueSchema).alias("sample")
            )
            .filter(col("sample.fullDocument.account_id") != "null")
            .select("sample.fullDocument.*", "sample.clusterTime")
        )
        # Converting the unix timestamp
        trade_df = (
            value_df.select("symbol", "ClusterTime")
            .withColumn(
                "CreatedTime",
                to_timestamp(
                    from_unixtime(
                        substring("ClusterTime", 1, 10).cast(LongType()),
                        "yyyy-MM-dd HH:mm:ss",
                    )
                ),
            )
            .drop("ClusterTime")
        )
        # Using window aggregations
        window_agg_df = (
            trade_df.withColumn(
                "current_timestamp",
                to_timestamp(current_timestamp(), "yyyy-MM-dd HH:mm:ss"),
            )
            .groupBy("symbol", window(col("CreatedTime"), "5 minute"))
            .count()
        )

        window_agg_df.select(
            "window.start", "window.end", "symbol", "current_timestamp", "count"
        ).createOrReplaceTempView("updates")

        final_df = SPARK_INSTANCE.sql(
            """
                                select symbol,count from updates where
                                current_timestamp between start and end
                                 order by count desc limit 3 """
        )

        processudf = udf(self.process, IntegerType())
        index_df = final_df.withColumn("index", processudf("symbol"))
        return index_df
