from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.functions import trim, when, upper, current_date, expr, regexp_replace, substring
from pyspark.sql.window import Window


@dp.temporary_view(
    name="products_silver_staging", comment="Transformed trips data ready for CDC upsert"
)
@dp.expect("valid_date", "prd_start_dt >= '2002-01-01'")
def silver_crm_products():
    df_bronze = spark.readStream.option("ignoreDeletes", "true").table("kue_data_warehousing_source.bronze.crm_products")
    df_bronze = df_bronze.where(F.col("prd_start_dt").isNotNull())

    df_silver= df_bronze.fillna({"prd_cost":0}).withColumn("prd_sk",expr("substring(prd_key, 7, length(prd_key))"))\
                  .withColumn("prod_cat_sk",regexp_replace(substring(df_bronze.prd_key, 1, 5),"-","_"))\
                  .withColumn("prd_line",when(upper(trim(df_bronze.prd_line)) =="R","Road Bike")\
                                            .when(upper(trim(df_bronze.prd_line)) =="S","Savy Bike")\
                                            .when(upper(trim(df_bronze.prd_line)) =="M","Mountain Bike")\
                                            .when(upper(trim(df_bronze.prd_line)) =="R","Tipper Bike")\
                                            .otherwise("Other"))\
                  .withColumn("dwd_date",current_date())\
                  .withColumn("silver_processed_timestamp", F.current_timestamp())

    return df_silver


dp.create_streaming_table(
    name="kue_data_warehousing_source.silver.crm_products",
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
    target="kue_data_warehousing_source.silver.crm_products",
    source="products_silver_staging",
    keys=["prd_sk"],
    sequence_by=F.col("silver_processed_timestamp"),
    stored_as_scd_type=1,
    except_column_list=[],
)
