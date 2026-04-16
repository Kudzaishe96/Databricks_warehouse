from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.functions import trim, when, upper, current_date
from pyspark.sql.window import Window


@dp.temporary_view(
    name="customer_silver_staging", comment="Transformed trips data ready for CDC upsert"
)
@dp.expect("valid_date", "cst_create_date >= '2025-01-01'")
def silver_crm_customers():
    df_bronze = spark.readStream.option("ignoreDeletes", "true").table("kue_data_warehousing_source.bronze.crm_customer")
    df_bronze = df_bronze.where(F.col("cst_create_date").isNotNull())

    df_silver = df_bronze.where(df_bronze.cst_id.isNotNull())\
                        .withColumn("cst_firstname",trim(df_bronze.cst_firstname))\
                        .withColumn("cst_lastname",trim(df_bronze.cst_lastname))\
                        .withColumn("cst_marital_status",when(upper(trim(df_bronze.cst_marital_status)) == "M","Married")\
                        .when(upper(trim(df_bronze.cst_marital_status)) == "S","Single").otherwise("Other"))\
                        .withColumn("cst_gndr",when(upper(trim(df_bronze.cst_gndr)) == "M","Male")\
                        .when(upper(trim(df_bronze.cst_gndr ))== "F","Female").otherwise("Other"))\
                        .withColumn("dwd_date",current_date())\
                        .withColumn("silver_processed_timestamp", F.current_timestamp())

    return df_silver


dp.create_streaming_table(
    name="kue_data_warehousing_source.silver.crm_customer",
    comment="Cleaned and validated orders with CDC upsert capability",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)

dp.create_auto_cdc_flow(
    target="kue_data_warehousing_source.silver.crm_customer",
    source="customer_silver_staging",
    keys=["cst_id"],
    sequence_by=F.col("silver_processed_timestamp"),
    stored_as_scd_type=1,
    except_column_list=[],
)
