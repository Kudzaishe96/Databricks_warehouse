from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.functions import trim, when, upper, current_date, expr, regexp_replace, substring, lead, asc,length,to_date
from pyspark.sql.window import Window


@dp.temporary_view(
    name="sales_silver_staging", comment="Transformed trips data ready for CDC upsert"
)
@dp.expect("valid_date", "sls_order_dt >= '2013-01-01'")
def silver_crm_sales():
    df_bronze = spark.readStream.option("ignoreDeletes", "true").table("kue_data_warehousing_source.bronze.crm_sales")
    df_bronze = df_bronze.where(F.col("sls_order_dt").isNotNull())

    df_silver = df_bronze.withColumn("sls_order_dt",when((df_bronze.sls_order_dt == 0)|(length(df_bronze.sls_order_dt) !=8),None)\
                                           .otherwise(to_date(df_bronze.sls_order_dt, 'yyyyMMdd')))\
             .withColumn("sls_ship_dt",when((df_bronze.sls_ship_dt == 0)|(length(df_bronze.sls_ship_dt) !=8),None)\
                                           .otherwise(to_date(df_bronze.sls_ship_dt, 'yyyyMMdd')))\
             .withColumn("sls_due_dt",when((df_bronze.sls_due_dt == 0)|(length(df_bronze.sls_due_dt) !=8), None)\
                                           .otherwise(to_date(df_bronze.sls_due_dt, 'yyyyMMdd')))\
            .withColumn('dwd_date',current_date())\
            .withColumn("silver_processed_timestamp", F.current_timestamp())
    return df_silver


dp.create_streaming_table(
    name="kue_data_warehousing_source.silver.crm_sales",
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
    target="kue_data_warehousing_source.silver.crm_sales",
    source="sales_silver_staging",
    keys=["sls_ord_num"],
    sequence_by=F.col("silver_processed_timestamp"),
    stored_as_scd_type=1,
    except_column_list=[],
)
