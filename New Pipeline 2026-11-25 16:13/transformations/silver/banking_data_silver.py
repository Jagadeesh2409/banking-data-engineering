from pyspark import pipelines as dp
from pyspark.sql.functions import *


@dp.table(
    name="banking.silver.bank_data",
    comment="Silver bank branch data sourced from Bronze"
)
@dp.expect("valid_branch_id", "branch_id IS NOT NULL")
@dp.expect("valid_city", "city IS NOT NULL AND TRIM(city) != ''")
@dp.expect("valid_region", "region IS NOT NULL AND TRIM(region) != ''")
@dp.expect("valid_revenue", "firm_revenue IS NULL OR firm_revenue >= 0")
@dp.expect("valid_expenses", "expenses IS NULL OR expenses >= 0")
def bank_data():
    df = spark.read.table('banking.bronze.bank_data')
    df = df.dropDuplicates(["branch_id"])
    df = df.select(
        col("Branch_ID").cast("int").alias("branch_id"),
        trim(col("City")).alias("city"),
        trim(col("Region")).alias("region"),
        col("Firm_Revenue").cast("decimal(18,2)").alias("firm_revenue"),
        col("Expenses").cast("decimal(18,2)").alias("expenses"),
        col("Profit_Margin").cast("decimal(10,2)").alias("profit_margin"),
        col("_rescued_data"),
        col("injection_date"),
        col("source_file"))
    
    df = df.withColumn('revenue_missing',when(col("firm_revenue").isNull(),1).otherwise(0))
    return df







    

