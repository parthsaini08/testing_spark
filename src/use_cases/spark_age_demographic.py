import spark_consumer as sp_con
from pyspark.sql.functions import col, current_date, datediff, floor, from_json, udf
from pyspark.sql.types import DateType, IntegerType, LongType, TimestampType

# Class that returns customers divided into age_groups


class AgeDemographic:
    def __init__() -> None:
        pass

    def __new__(self, SPARK_INSTANCE, base_df, topic):
        # setting the payload schema
        valueSchema = sp_con.PayloadType(topic)
        # Selecting the value from key-value pair based on the defined schema
        df = base_df.select(
            from_json(col("value"), valueSchema).alias("sample")
        ).select("sample.fullDocument.birthdate")

        df = df.withColumn(
            "birthdate",
            ((col("birthdate").cast(LongType()) / 1000).cast(TimestampType())).cast(
                DateType()
            ),
        )
        # store age relative to current date
        df = df.withColumn(
            "age",
            floor(datediff(current_date(), col("birthdate")) / 365.25).cast(
                IntegerType()
            ),
        )
        # predefined age_ranges
        age_range = udf(
            lambda age: "< 18"
            if int(age or 0) < 18
            else "18-25"
            if (int(age or 0) >= 18 and int(age or 0) < 25)
            else "25-35"
            if (int(age or 0) >= 25 and int(age or 0) < 35)
            else "35-45"
            if (int(age or 0) >= 35 and int(age or 0) < 45)
            else "45-55"
            if (int(age or 0) >= 45 and int(age or 0) < 55)
            else "55-65"
            if (int(age or 0) >= 55 and int(age or 0) < 65)
            else "65-75"
            if (int(age or 0) >= 65 and int(age or 0) < 75)
            else "75+"
            if (int(age or 0) >= 75)
            else ""
        )
        df = df.withColumn("age_range", age_range(df.age))
        # aggregating count of customer ages by respective age_range
        df = df.groupBy("age_range").count()

        return df
