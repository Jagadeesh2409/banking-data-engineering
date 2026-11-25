from pyspark import pipelines as dp
from pyspark.sql.functions import *

@dp.table(
    name="banking.gold.branch_performance",
    comment="Gold business analytics for bank branch profitability"
)
def branch_performance():

    df = spark.read.table("banking.silver.bank_data")

    df = df.withColumn(
        "calculated_profit",
        col("firm_revenue") - col("expenses")
    )

    df = df.withColumn(
        "calculated_profit_margin",
        round(
            (col("calculated_profit") / col("firm_revenue")) * 100,
            2
        )
    )

    df = df.select(
        "branch_id",
        "city",
        "region",
        "firm_revenue",
        "expenses",
        "calculated_profit",
        "calculated_profit_margin",
        "revenue_missing"
    )

    return df