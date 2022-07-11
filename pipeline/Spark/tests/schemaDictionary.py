from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType


class schemaDictionary:
    def __init__(self) -> None:
        pass

    # New instance takes the type variable and returns the StructType accordingly
    def __new__(self, topic) -> StructType:
        # schema for the customers documents
        product_schema = StructType(
            [
                StructField("product", StringType()),
                StructField("count", LongType()),
            ]
        )
        price_schema = StructType(
            [
                StructField("symbol", StringType(), True),
                StructField("price", DoubleType(), True),
                StructField("clusterTime", StringType(), True),
            ]
        )
        age_schema = StructType(
            [
                StructField("age_range", StringType()),
                StructField("count", LongType()),
            ]
        )
        count_schema = StructType([StructField("COUNT", LongType(), True)])
        wealth_schema = StructType(
            [
                StructField("account_id", StringType()),
                StructField("total", DoubleType()),
            ]
        )
        schema_dict = {
            "product_relative_reach": product_schema,
            "price_at_an_instant": price_schema,
            "age_demographic": age_schema,
            "document_count": count_schema,
            "net_wealth": wealth_schema,
        }
        if topic in schema_dict.keys():
            return schema_dict[topic]
        else:
            print("Usecase not defined.")
            exit(0)
