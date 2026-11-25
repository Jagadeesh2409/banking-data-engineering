from pyspark import pipelines as dp
from pyspark.sql.functions import *


@dp.table(
    name="banking.gold.customer_analytics",
    comment="Gold customer analytics dataset with customer segmentation"
)
def customer_analytics():

    df = spark.read.table("banking.silver.customer_data")

    df = df.withColumn(
        "age_group",
        when(col("age").isNull(), "Unknown")
        .when(col("age") < 25, "Young Adult")
        .when(col("age") <= 40, "Adult")
        .when(col("age") <= 60, "Middle Aged")
        .otherwise("Senior")
    )

    df = df.withColumn(
        "city_missing",
        when(col("city").isNull(), 1).otherwise(0)
    )

    return df.select(
        "customer_id",
        "age",
        "age_group",
        "customer_type",
        "city",
        "region",
        "bank_name",
        "branch_id",
        "age_missing",
        "customer_type_missing",
        "city_missing"
    )





