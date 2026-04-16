from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.functions import trim, when, upper, current_date, expr, regexp_replace, substring, lead, asc,length,to_date
from pyspark.sql.window import Window


@dp.temporary_view(
    name="erp_products_silver_staging", comment="Transformed trips data ready for CDC upsert"
)
def silver_erp_products():
    df_bronze = spark.readStream.option("ignoreDeletes", "true").table("kue_data_warehousing_source.bronze.erp_products")

    df_silver = df_bronze.withColumnRenamed("ID","id").withColumnRenamed("CAT","category")\
                                     .withColumnRenamed("SUBCAT","sub_category").withColumnRenamed("MAINTENANCE","Maintenance")\
                                    .withColumn("silver_processed_timestamp", F.current_timestamp())
    return df_silver


dp.create_streaming_table(
    name="kue_data_warehousing_source.silver.erp_products",
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
    target="kue_data_warehousing_source.silver.erp_products",
    source="erp_products_silver_staging",
    keys=["id"],
    sequence_by=F.col("silver_processed_timestamp"),
    stored_as_scd_type=1,
    except_column_list=[],
)
