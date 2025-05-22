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
prcl_df = spark.read.option("readChangeData", "true").option("startingVersion", brnz_mx_version).table("rehouzd_db.bronze.brnz_prcl_prop_sales_dtl").filter((col("prop_acty_status_cd").isin(['SALE','LISTING','RENTAL'])) & (col("_change_type").isin(["insert","update_postimage"]))).select( 'brnz_prcl_prop_sales_dtl_sk','prop_attr_br_cnt','prop_attr_bth_cnt','prop_attr_sqft_nr','prop_yr_blt_nr','prop_zip_cd','prop_city_nm','prop_state_nm','prop_address_line_txt','prop_acty_status_cd','prop_acty_sub_status_cd')

prcl_df = prcl_df.withColumn(
    "flag_info",
    when((col("prop_acty_status_cd")=='SALE')&(col("prop_acty_sub_status_cd").isin(['SOLD','NON_ARMS_LENGTH_TRANSFER','SOLD_INTER_PORTFOLIO_TRANSFER','NON_ARMS_LENGTH_INTRA_PORTFOLIO_TRANSFER'])),lit(True)).when((col("prop_acty_status_cd")=='LISTING')&(col("prop_acty_sub_status_cd").isin([ 'LISTED_SALE', 'LISTING_REMOVED', 'OTHER', 'PENDING_SALE','PRICE_CHANGE', 'RELISTED'])),lit(True)).when((col("prop_acty_status_cd")=='RENTAL')&(col("prop_acty_sub_status_cd").isin(['LISTED_FOR_RENT','PRICE_CHANGE','DELISTED_FOR_RENT','LISTED_RENT'])),lit(True)).otherwise(lit(False))
)


# COMMAND ----------

investor_profile_schema = StructType([
    StructField("min_sqft", IntegerType()),
    StructField("min_year", IntegerType()),
    StructField("list_zips", ArrayType(StringType())),
    StructField("mx_props_amnt", FloatType()),
    StructField("min_props_amnt", FloatType()),
    
    StructField("prop_bth_cnd_nm", MapType(StringType(), IntegerType())),
    StructField("prop_ext_cnd_nm", MapType(StringType(), IntegerType())),
    StructField("prop_int_cnd_nm", MapType(StringType(), IntegerType())),
    StructField("prop_kth_cnd_nm", MapType(StringType(), IntegerType())),
    StructField("prop_tlt_cnd_nm", MapType(StringType(), IntegerType())),

    StructField("min_prop_attr_br_cnt", IntegerType()),
    StructField("min_prop_attr_bth_cnt", IntegerType())
])

slvr_df = spark.read.table("rehouzd_db.silver.slvr_int_inv_dtl").filter(
    (col('active_rec_ind')==True) & (col('end_timestamp').isNull()==True) &  (col("active_flg")==True)
    ).withColumn("investor_profile",from_json("investor_profile", investor_profile_schema)).selectExpr("slvr_int_inv_dtl_sk", "investor_profile.min_sqft as min_sqft", "investor_profile.min_year as min_year", "investor_profile.list_zips as list_zips", "investor_profile.min_prop_attr_br_cnt as attr_br_cnt", "investor_profile.min_prop_attr_bth_cnt as attr_bth_cnt")



# COMMAND ----------

slv_fk,etl_nr = spark.sql("select coalesce(max(slvr_int_prop_sk),0 ) as slvr_int_prop_sk , coalesce(max(etl_nr),0 ) as etl_nr from rehouzd_db.silver.slvr_int_prop").collect()[0]
slv_fk,etl_nr

# COMMAND ----------

result_df = prcl_df.alias("brnz").join(
    slvr_df.alias("slv"),
    on =(
        (col("brnz.prop_attr_br_cnt") == col("slv.attr_br_cnt")) &
        (col("brnz.prop_attr_bth_cnt") == col("slv.attr_bth_cnt")) &
        (col("brnz.prop_attr_sqft_nr") == col("slv.min_sqft")) &
        (col("brnz.prop_yr_blt_nr") >= col("slv.min_year")) &
        (array_contains(col('slv.list_zips'), col('brnz.prop_zip_cd')))
    ),
    how = 'left'
).select(
    "brnz.brnz_prcl_prop_sales_dtl_sk",
    "slv.slvr_int_inv_dtl_sk",
    "brnz.prop_attr_br_cnt",
    "brnz.prop_attr_bth_cnt",
    "brnz.prop_attr_sqft_nr",
    "brnz.prop_yr_blt_nr",
    "brnz.prop_zip_cd",
    "brnz.prop_city_nm",
    "brnz.prop_state_nm",
    "brnz.prop_address_line_txt" 
).withColumn("src_system_cd",lit("PAR")).withColumn("src_system_dc",lit("PROPSTREAM")).withColumn("slvr_int_prop_sk", lit(slv_fk)+ monotonically_increasing_id()).withColumn("load_date_dt",current_date()).withColumn("record_inserted_ts",current_timestamp()).withColumn("etl_nr",lit(etl_nr))


# COMMAND ----------

result_df.write.mode("append").saveAsTable("rehouzd_db.silver.slvr_int_prop")