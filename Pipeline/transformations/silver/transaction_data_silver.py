from pyspark import pipelines as dp
from pyspark.sql.functions import *

@dp.table(
name="banking.silver.transaction_data",
comment="Silver transaction data cleaned and standardized from Bronze"
)
@dp.expect("valid_transaction_id", "transaction_id IS NOT NULL")
@dp.expect("valid_customer_id", "customer_id IS NOT NULL")
@dp.expect("valid_account_type", "account_type IS NOT NULL AND TRIM(account_type) != ''")
@dp.expect("valid_transaction_amount", "transaction_amount >= 0")
@dp.expect("valid_total_balance", "total_balance >= 0")
@dp.expect("valid_investment_amount", "investment_amount >= 0")
@dp.expect("valid_transaction_date", "transaction_date IS NOT NULL")
def transaction_data():

    df = spark.read.table("banking.bronze.transaction_data")

    df = df.select(
        col("Transaction_ID").cast("bigint").alias("transaction_id"),
        col("Customer_ID").cast("bigint").alias("customer_id"),
        trim(col("Account_Type")).alias("account_type"),
        col("Total_Balance").cast("decimal(18,2)").alias("total_balance"),
        col("Transaction_Amount").cast("decimal(18,8)").alias("transaction_amount"),
        col("Investment_Amount").cast("decimal(18,2)").alias("investment_amount"),
        trim(col("Investment_Type")).alias("investment_type"),
        to_date(col("Transaction_Date")).alias("transaction_date"),
        col("_rescued_data"),
        col("injection_date"),
        col("source_file")
    )

    return df
    