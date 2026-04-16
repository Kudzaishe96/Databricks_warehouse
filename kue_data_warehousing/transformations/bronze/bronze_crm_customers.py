from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.functions import md5, concat_ws, sha2

# Configuration
SOURCE_PATH = "/Volumes/kue_data_warehousing_source/crm_source/dw_crm_source/source_crm/customer/"

# https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/schema
 
@dp.table(
    name="kue_data_warehousing_source.bronze.crm_customer",
    comment="Streaming ingestion for CRM Customer Data",
    table_properties={
        "quality": "bronze",
        "layer": "bronze",
        "source_format": "csv",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def crm_customer_bronze():
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.maxFilesPerTrigger", 1)
        .load(SOURCE_PATH)
    )

    df = df.withColumn("file_name", col("_metadata.file_path"))\
        .withColumn("ingest_datetime", current_timestamp())
    
    return df
