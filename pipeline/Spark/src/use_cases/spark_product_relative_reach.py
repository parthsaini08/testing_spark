import spark_consumer as sp_con
from pyspark.sql.functions import col, explode, from_json,from_unixtime,substring,to_timestamp


# Class that finds the product relative reach
class productRelativeReach:
    def __init__() -> None:
        pass

    def __new__(self, SPARK_INSTANCE, base_df, topic):
        # setting the payload schema
        valueSchema = sp_con.PayloadType(topic)
        # Selecting the value from key-value pair based on the defined schema
        df = base_df.select(
            from_json(col("value"), valueSchema).alias("sample")
        ).select("sample.fullDocument.products","sample.clusterTime")
        # Splitting the product array into separate rows. Each row contains a product
        df2 = df.withColumn("CreatedTime",to_timestamp(from_unixtime(col("clusterTime") / 1000))).withWatermark("CreatedTime","2 minutes").select(explode(df.products))
        # Renaming the column with the products as "product"
        df3 = df2.selectExpr("col as product")
        # Counting the products
        productCount = df3.groupBy("product").count()

        return productCount
