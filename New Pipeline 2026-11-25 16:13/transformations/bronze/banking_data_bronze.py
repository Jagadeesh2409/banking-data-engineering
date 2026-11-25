from pyspark import pipelines as dp
from pyspark.sql.functions import *

# Bank data
@dp.table(name="bank_data")
def bank_data():
    return (
        spark.readStream.format("cloudFiles").option("cloudFiles.format", "csv").option("header", True).option("cloudFiles.schemaEvolutionMode", "rescue").load("s3://project-source-datas/Banking Data/bank/")).withColumn("injection_date",current_timestamp()).withColumn("source_file",col('_metadata.file_name'))





