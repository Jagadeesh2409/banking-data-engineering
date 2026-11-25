from pyspark import pipelines as dp
from pyspark.sql.functions import *

# Transaction data
@dp.table(name="transaction_data")
def transaction_data():
    return (spark.readStream.format("cloudFiles").option("cloudFiles.format", "csv").option("header", True).option("cloudFiles.schemaEvolutionMode", "rescue").load("s3://project-source-datas/Banking Data/transaction/")).withColumn("injection_date",current_timestamp()).withColumn("source_file",col('_metadata.file_name'))