# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

mx_etl_nr = spark.sql('select coalesce(max(etl_nr),0 ) as etl_nr from  rehouzd_db.bronze.brnz_prps_prop_sales_dtl').collect()[0][0]
mx_id = spark.sql('select coalesce(max(slv_prps_prop_pk),0 ) as etl_nr from  rehouzd_db.silver.slv_prps_prop_sales_dtl').collect()[0][0] + 1

bnz_df = spark.read.table("rehouzd_db.bronze.brnz_prps_prop_sales_dtl") \
    .filter(col('etl_nr') == mx_etl_nr) \
    .select(
        "investor_company_nm_txt",
        "prop_last_sale_dt",
        "prop_last_sale_amt",
        "prop_attr_br_cnt",
        "prop_attr_bth_cnt",
        "prop_attr_sqft_nr",
        "prop_yr_blt_nr",
        "prop_address_line_txt",
        "prop_city_nm",
        "prop_state_nm",
        "prop_cnty_nm",
        "prop_zip_cd",
        "prop_tlt_cnd_nm",
        "prop_int_cnd_nm",
        "prop_ext_cnd_nm",
        "prop_bth_cnd_nm",
        "prop_kth_cnd_nm",
        "prop_list_price_amt",
        "year",
        concat(
            col("prop_address_line_txt"),
            lit("/"),
            col("prop_city_nm"),
            lit("/"),
            col("prop_state_nm"),
            lit("/"),
            col("prop_zip_cd")
        ).alias("address_key")
    )


# COMMAND ----------

bnz_df.count()

# COMMAND ----------

bd_rec_cnd = '''investor_company_nm_txt is not null
            AND prop_attr_br_cnt is not null
            AND prop_attr_bth_cnt is not null
            AND prop_last_sale_amt is not null
            AND prop_yr_blt_nr is not null
            AND prop_attr_sqft_nr is not null
            AND prop_zip_cd is not null'''
good_rec_df = bnz_df.where(bd_rec_cnd)
bad_rec_df = bnz_df.subtract(good_rec_df)
bad_rec_df.write.format("delta").option("mergeSchema","true").mode("overwrite").saveAsTable("rehouzd_db.bronze.brnz_prps_prop_sales_db_rec")
bnz_df = good_rec_df


# COMMAND ----------

bnz_df.count()

# COMMAND ----------

slvr_df = spark.read.table("rehouzd_db.silver.slv_prps_prop_sales_dtl").select("*" ,concat(
            col("prop_address_line_txt"),
            lit("/"),
            col("prop_city_nm"),
            lit("/"),
            col("prop_state_nm"),
            lit("/"),
            col("prop_zip_cd")
        ).alias("address_key"))



# COMMAND ----------

res_df = bnz_df.join(slvr_df, on=["investor_company_nm_txt", "address_key"], how="leftanti").drop("address_key").withColumn(
    "slv_prps_prop_pk",
    lit(mx_id) + monotonically_increasing_id()
).withColumn("load_date_dt",current_date()).withColumn("record_inserted_ts",current_timestamp())

# COMMAND ----------

if res_df.first():
    print("New records to insert")
    res_df.write.mode("append").saveAsTable("rehouzd_db.silver.slv_prps_prop_sales_dtl")
else:
    print("No new records to insert")