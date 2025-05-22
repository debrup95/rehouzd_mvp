# Databricks notebook source
import json
import requests
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime , timedelta
import math
from utils.common_utils import *
import logging
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, ArrayType, StringType, MapType


# COMMAND ----------

brnz_mx_version = get_max_version(spark,"rehouzd_db.bronze.brnz_prcl_prop_sales_dtl")
prcl_df = spark.read.option("readChangeData", "true").option("startingVersion", brnz_mx_version).table("rehouzd_db.bronze.brnz_prcl_prop_sales_dtl").filter((col("prop_acty_status_cd").isin(['SALE','LISTING','RENTAL'])) & (col("_change_type").isin(["insert","update_postimage"])))

# COMMAND ----------

etl_nr = spark.sql("select coalesce(max(etl_nr),0 ) as etl_nr from rehouzd_db.silver.slvr_int_prop").collect()[0]['etl_nr']
slv_prp = spark.read.table("rehouzd_db.silver.slvr_int_prop").filter(col("etl_nr") == etl_nr)

# COMMAND ----------

etl_nr,slvr_int_prop_comps_sk = spark.sql("select coalesce(max(etl_nr),0 ) as etl_nr , coalesce(max(slvr_int_prop_comps_sk),0) as slvr_int_prop_comps_sk from rehouzd_db.silver.slvr_int_prop_comps").collect()[0]

# COMMAND ----------

etl_nr += 1; slvr_int_prop_comps_sk += 1

# COMMAND ----------

result_df = prcl_df.alias("bnz").join(
    slv_prp.alias("slvr"),
    on = (
        (col("bnz.prop_attr_br_cnt") == col("slvr.prop_attr_br_cnt")) &
        (col("bnz.prop_attr_bth_cnt") == col("slvr.prop_attr_bth_cnt")) &
        (col("bnz.prop_attr_sqft_nr") >= col("slvr.prop_attr_sqft_nr") - 200) & 
        (col("bnz.prop_attr_sqft_nr") <= col("slvr.prop_attr_sqft_nr") + 200) &
        (col("bnz.prop_yr_blt_nr") >= col("slvr.prop_yr_blt_nr") - 20) & 
        (col("bnz.prop_yr_blt_nr") <= col("slvr.prop_yr_blt_nr") + 20)
    ),
    how='inner'
).selectExpr(
    "bnz.prop_attr_br_cnt",
    "slvr.slvr_int_prop_sk",
    "bnz.brnz_prcl_prop_sales_dtl_sk",
    "bnz.prop_attr_bth_cnt",
    "bnz.prop_attr_sqft_nr",
    "bnz.prop_yr_blt_nr",
    "bnz.prop_address_line_txt",
    "bnz.prop_city_nm",
    "bnz.prop_state_nm",
    "bnz.prop_cnty_nm",
    "bnz.prop_zip_cd",
    "bnz.prop_latitude_val", 
    "bnz.prop_longitude_val",
    "bnz.prop_acty_status_cd",
    "bnz.prop_sale_amt"
).withColumn("prop_latest_rental_amt", when(col('prop_acty_status_cd') == 'RENTAL', col('prop_sale_amt')).otherwise(0)).withColumn("prop_latest_sales_amt", when(col('prop_acty_status_cd') == 'SALE', col('prop_sale_amt')).otherwise(0)).withColumn(
    "etl_nr",
    lit(etl_nr)
).withColumn(
    "slvr_int_prop_comps_sk",
    lit(slvr_int_prop_comps_sk) + monotonically_increasing_id()
).withColumn(
    "load_date_dt",
    current_date()
).withColumn(
    "etl_recorded_gmts",
    current_timestamp()
).withColumn(
    "record_inserted_ts",
    current_timestamp()
)

# COMMAND ----------

result_df.write.format("delta").mode("append").saveAsTable("rehouzd_db.silver.slvr_int_prop_comps")