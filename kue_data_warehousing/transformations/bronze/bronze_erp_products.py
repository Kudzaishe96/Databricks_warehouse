from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.functions import md5, concat_ws, sha2

# Configuration
SOURCE_PATH = "/Volumes/kue_data_warehousing_source/erp_source/dw_erp_source/products/"

# https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/schema
 
@dp.table(
    name="kue_data_warehousing_source.bronze.erp_products",
    comment="Streaming ingestion for ERP Products Catergory Data",
    table_properties={
        "quality": "bronze",
        "layer": "bronze",
        "source_format": "csv",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def erp_products_bronze():
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.schemaLocation", "/Volumes/kue_data_warehousing_source/erp_source/dw_erp_source/checkpoints/erp_products_schema")
        .option("cloudFiles.maxFilesPerTrigger", 1)
        .load(SOURCE_PATH)
    )

    df = df.withColumn("file_name", col("_metadata.file_path"))\
        .withColumn("ingest_datetime", current_timestamp())
    
    return df
