# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import json
from datetime import datetime , timedelta
import math
from utils.common_utils import *
import logging

# COMMAND ----------

etl_nr = spark.sql('select coalesce(max(etl_nr), 0) as etl_nr from rehouzd_db.silver.slvr_int_prop_comps').collect()[0].etl_nr

# COMMAND ----------

slv_sk ,etl_nr = spark.sql('select coalesce(max(slvr_int_prop_dtl_sk), 0) as slvr_int_prop_dtl_sk , coalesce(max(etl_nr), 0) as etl_nr from silver.slvr_int_prop_sales_dlt').collect()[0]

brnz_mx_version = get_max_version(spark,"bronze.brnz_prcl_prop_sales_dtl")

brnz_df = spark.read.option("readChangeData", "true").option("startingVersion", brnz_mx_version).table("bronze.brnz_prcl_prop_sales_dtl").select("prop_sale_dt","prop_sale_amt","prop_list_price_amt","brnz_prcl_prop_sales_dtl_sk")



slv_sk ,etl_nr = spark.sql('select coalesce(max(slvr_int_prop_dtl_sk), 0) as slvr_int_prop_dtl_sk , coalesce(max(etl_nr), 0) as etl_nr from silver.slvr_int_prop_sales_dlt').collect()[0]

etl_nr += 1; slv_sk += 1

etl_nr = spark.sql('select coalesce(max(etl_nr), 0) as etl_nr from rehouzd_db.silver.slvr_int_prop_comps').collect()[0].etl_nr
slvr_df = spark.read.table("rehouzd_db.silver.slvr_int_prop_comps").filter(col("etl_nr") == etl_nr).selectExpr("brnz_prcl_prop_sales_dtl_sk")
res_df = brnz_df.alias("brnz").join(
    slvr_df.alias("slvr"), 
    on = "brnz_prcl_prop_sales_dtl_sk",
    how  = 'inner'
).selectExpr(
    "Null asslvr_int_prop_sk",
    "brnz.prop_sale_dt",
    "brnz.prop_sale_amt",
    "NULL as prop_tlt_cnd_nm",
    "NULL as prop_int_cnd_nm", 
    "NULL as prop_ext_cnd_nm", 
    "NULL as prop_bth_cnd_nm", 
    "NULL as prop_kth_cnd_nm",
    "brnz.prop_list_price_amt",
    "true as latest_record_ind",
    "Null as usraddr"
).withColumn(
    "etl_nr",
    lit(etl_nr).cast('bigint')
).withColumn(
    "slvr_int_prop_comps_sk",
    lit(slv_sk) + monotonically_increasing_id()
).withColumn(
    "load_date_dt",
    current_date()
).withColumn(
    "etl_recorded_gmts",
    current_timestamp()
).withColumn(
    "record_inserted_ts",
    current_timestamp()
).withColumn("prop_list_price_amt",col("prop_list_price_amt").cast("double"))

# COMMAND ----------

display(res_df)

# COMMAND ----------

res_df.printSchema()

# COMMAND ----------

res_df.write.mode("append").option("mergeSchema", "true").saveAsTable("silver.slvr_int_prop_sales_dlt")