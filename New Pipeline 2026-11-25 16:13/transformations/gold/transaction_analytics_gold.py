from pyspark import pipelines as dp
from pyspark.sql.functions import *


@dp.table(
    name="banking.gold.transaction_analytics",
    comment="Gold transaction analytics dataset with amount classification"
)
def transaction_analytics():

    df = spark.read.table("banking.silver.transaction_data")

    df = df.withColumn(
        "transaction_category",
        when(col("transaction_amount") < 2000, "Low")
        .when(col("transaction_amount") <= 5000, "Medium")
        .otherwise("High")
    )

    return df.select(
        "transaction_id",
        "customer_id",
        "account_type",
        "total_balance",
        "transaction_amount",
        "transaction_category",
        "investment_amount",
        "investment_type",
        "transaction_date"
    )