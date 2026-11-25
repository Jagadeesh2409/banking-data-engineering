from pyspark import pipelines as dp
from pyspark.sql.functions import *

@dp.table(
name="banking.silver.customer_data",
comment="Silver customer data cleaned and standardized from Bronze"
)
@dp.expect("valid_customer_id", "customer_id IS NOT NULL")
@dp.expect("valid_city", "city IS NULL OR TRIM(city) != ''")
@dp.expect("valid_customer_type", "customer_type IS NULL OR TRIM(customer_type) != ''")
@dp.expect("valid_age", "age IS NULL OR (age >= 18 AND age <= 100)")
def customer_data():

    df = spark.read.table("banking.bronze.customer_data")

    # Deduplicate
    df = df.dropDuplicates(["Customer_ID"])

    # Standardize schema
    df = df.select(
        col("Customer_ID").cast("int").alias("customer_id"),
        col("Age").cast("int").alias("age"),
        trim(col("Customer_Type")).alias("customer_type"),
        trim(col("City")).alias("city"),
        trim(col("Region")).alias("region"),
        trim(col("Bank_Name")).alias("bank_name"),
        col("Branch_ID").cast("int").alias("branch_id")
    )

    # Data quality flags
    df = df.withColumn(
        "age_missing",
        when(col("age").isNull(), 1).otherwise(0)
    )

    df = df.withColumn(
        "customer_type_missing",
        when(col("customer_type").isNull(), 1).otherwise(0)
    )

    df = df.withColumn(
        "city_missing",
        when(col("city").isNull(), 1).otherwise(0)
    )

    return df