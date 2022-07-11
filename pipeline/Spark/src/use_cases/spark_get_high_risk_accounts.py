import spark_consumer as sp_con
from pyspark.sql.functions import (
    col,
    current_timestamp,
    expr,
    from_json,
    from_unixtime,
    to_timestamp,
)

MIN_TRANSACTIONS = 3.0
Z_THRESH = 1.65  # ~Top 5% transaction amounts

# Need to change these based on required scenario

# Determines how late a transaction can be processed
TRANSACTION_DELAY_THRESHOLD = "interval 5 minutes"

# Determines how old mean of an account is acceptable
STATS_DELAY_THRESHOLD = "interval 20 minutes"


class OutlierDetector:
    def __init__(self):
        pass

    def __new__(self, SPARK_INSTANCE, transactions_df, topic):
        valueSchema = sp_con.PayloadType(topic)

        STATS_TOPIC_NAME = "spark.transaction_stats"
        STATS_READING_OFFSET = "earliest"

        # Read the aggregated mean and stddev of transactions
        stats = sp_con.sparkConsumer(
            SPARK_INSTANCE, STATS_TOPIC_NAME, STATS_READING_OFFSET
        )

        statSchema = sp_con.StatType()

        stats = (
            stats.withColumn("data", from_json(col("value"), statSchema))
            .select("data.*")
            .withWatermark("last_updated", STATS_DELAY_THRESHOLD)
            .filter(
                (col("transactions_count") > MIN_TRANSACTIONS)
                & (
                    col("last_updated")
                    > current_timestamp() - expr(STATS_DELAY_THRESHOLD)
                )
            )
            .dropDuplicates(["account_id"])
        )

        # Filter the transactions data-frame
        transactions_df = (
            transactions_df.withColumn("data", from_json(col("value"), valueSchema))
            .select("data.*")
            .select(
                col("fullDocument.account_id").alias("cur_account_id"),
                "fullDocument.transaction_code",
                "fullDocument.total",
                to_timestamp(from_unixtime(col("clusterTime") / 1000)).alias(
                    "transaction_time"
                ),
            )
            .withWatermark("transaction_time", TRANSACTION_DELAY_THRESHOLD)
            .filter(col("transaction_code") == "buy")
        )

        transactions_df.printSchema()

        # Join currently streaming transactions and the stats_df
        transactions_df.join(
            stats,
            expr(
                """
            cur_account_id = account_id AND
            transaction_time > last_updated
        """
            ),
            "inner",
        ).withColumn(
            "z_score", (col("total") - col("mean_total")) / col("stddev_total")
        ).filter(
            col("z_score") >= Z_THRESH
        ).select(
            "account_id", "total", "z_score", "transaction_time"
        ).createOrReplaceTempView(
            "table"
        )

        time_formatted_outliers = SPARK_INSTANCE.sql(
            """
            SELECT account_id, total, z_score,
            DATE_FORMAT(transaction_time, 'dd/MM/yyyy hh:mm:ss a' ) AS transaction_time
            FROM table
            """
        )

        time_formatted_outliers.printSchema()

        return time_formatted_outliers
