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

brnz_mx_version = get_max_version(spark,"rehouzd_db.bronze.brnz_prps_prop_sales_dtl")

slv_sk ,etl_nr = spark.sql('select coalesce(max(slvr_int_prop_dtl_sk), 0) as slvr_int_prop_dtl_sk , coalesce(max(etl_nr), 0) as etl_nr from silver.slvr_int_prop_sales_dlt').collect()[0]

slv_sk +=1
etl_nr +=1

bnz_df = spark.read.option("readChangeData", "true").option("startingVersion", brnz_mx_version).table("rehouzd_db.bronze.brnz_prps_prop_sales_dtl").selectExpr(
    "Null as slvr_int_prop_fk",
    "PROP_LAST_SALE_DT as prop_sale_dt",
   "PROP_LAST_SALE_AMT as prop_sale_amt",
    "PROP_TLT_CND_NM as prop_tlt_cnd_nm" ,
    "PROP_INT_CND_NM as prop_int_cnd_nm" ,
    "PROP_EXT_CND_NM as prop_ext_cnd_nm",
    "PROP_BTH_CND_NM as prop_bth_cnd_nm",
    "PROP_KTH_CND_NM as prop_kth_cnd_nm",
    "PROP_LIST_PRICE_AMT as prop_list_price_amt",
    "CONCAT(PROP_ADDRESS_LINE_TXT,'/',PROP_CITY_NM,'/',PROP_STATE_NM,'/',PROP_CNTY_NM,'/',PROP_ZIP_CD) as usraddr",
)

window_spec = Window.partitionBy("usraddr").orderBy(desc("prop_sale_dt"))
bnz_df = bnz_df.withColumn(
    "d_rank",
    dense_rank().over(window_spec)
).withColumn(
    "latest_record_ind",
    when(col("d_rank")==1,lit(True)).otherwise(lit(False))
).drop("d_rank").withColumn("slvr_int_prop_dtl_sk", lit(slv_sk)+ monotonically_increasing_id()).withColumn("load_date_dt",current_date()).withColumn("record_inserted_ts",current_timestamp()).withColumn("etl_recorded_gmts",current_timestamp()).withColumn("etl_nr",lit(etl_nr))

# COMMAND ----------

from delta.tables import DeltaTable

slvrdf = DeltaTable.forName(spark,"silver.slvr_int_prop_sales_dlt")

slvrdf.alias("t").merge(
    bnz_df.alias("s"),
    "t.latest_record_ind = true and t.usraddr = s.usraddr"
).whenMatchedUpdate(
    set = {"t.latest_record_ind":lit(False)}
).whenNotMatchedInsertAll().execute()