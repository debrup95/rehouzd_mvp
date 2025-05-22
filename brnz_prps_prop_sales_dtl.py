# Databricks notebook source
path_dst = dbutils.widgets.get("path_dst")

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

schema = StructType([
    StructField("Investor Name", StringType(), True),
    StructField("Address", StringType(), True),
    StructField("City", StringType(), True),
    StructField("State", StringType(), True),
    StructField("Zip", StringType(), True),
    StructField("County", StringType(), True),
    StructField("APN", StringType(), True),
    StructField("Owner Occupied", StringType(), True),
    StructField("Mailing Address", StringType(), True),
    StructField("Mailing City", StringType(), True),
    StructField("Mailing State", StringType(), True),
    StructField("Mailing Zip", StringType(), True),
    StructField("Property Class", StringType(), True),
    StructField("Property Type", StringType(), True),
    StructField("Bedrooms", IntegerType(), True),
    StructField("Bathrooms", DoubleType(), True),
    StructField("Building Sqft", IntegerType(), True),
    StructField("Year Built", IntegerType(), True),
    StructField("Lot Size Sqft", IntegerType(), True),
    StructField("Last Sale Date", DateType(), True),
    StructField("Last Sale Amount", DoubleType(), True),
    StructField("Est. Value", IntegerType(), True),
    StructField("Est. Equity", FloatType(), True),
    StructField("Monthly Rent", IntegerType(), True),
    StructField("Gross Yield %", DoubleType(), True)
])

df = spark.read.format("csv").option("header", "true").schema(schema).load(f"{path_dst}/*")


# COMMAND ----------

brnz_prps_prop_sales_dtl_sk , etl_nr = spark.sql('''
          select coalesce(max(brnz_prps_prop_sales_dtl_sk), 0) as brnz_prps_prop_sales_dtl_sk,
          coalesce(max(etl_nr), 0) as etl_nr
          from bronze.brnz_prps_prop_sales_dtl
          ''').collect()[0]

brnz_prps_prop_sales_dtl_sk , etl_nr

# COMMAND ----------

df = df.withColumnRenamed("Investor Name", "INVESTOR_COMPANY_NM_TXT") \
       .withColumnRenamed("Last Sale Date", "PROP_LAST_SALE_DT") \
       .withColumnRenamed("Last Sale Amount", "PROP_LAST_SALE_AMT") \
       .withColumnRenamed("Bedrooms", "PROP_ATTR_BR_CNT") \
       .withColumnRenamed("Bathrooms", "PROP_ATTR_BTH_CNT") \
       .withColumnRenamed("Building Sqft", "PROP_ATTR_SQFT_NR") \
       .withColumnRenamed("Year Built", "PROP_YR_BLT_NR") \
       .withColumnRenamed("Address", "PROP_ADDRESS_LINE_TXT") \
       .withColumnRenamed("City", "PROP_CITY_NM") \
       .withColumnRenamed("State", "PROP_STATE_NM") \
       .withColumnRenamed("County", "PROP_CNTY_NM") \
       .withColumnRenamed("Zip", "PROP_ZIP_CD") \
       .withColumnRenamed("Property Class", "PROP_TLT_CND_NM") \
       .withColumnRenamed("Property Type", "PROP_INT_CND_NM") \
       .withColumnRenamed("Owner Occupied", "PROP_EXT_CND_NM") \
       .withColumnRenamed("Mailing Address", "PROP_BTH_CND_NM") \
       .withColumnRenamed("Mailing City", "PROP_KTH_CND_NM") \
       .withColumnRenamed("Mailing State", "PROP_LIST_PRICE_AMT") \
       .withColumnRenamed("INVESTOR_COMPANY_NM_TXT", "investor_company_nm_txt") \
       .withColumnRenamed("PROP_LAST_SALE_DT", "prop_last_sale_dt") \
       .withColumnRenamed("PROP_LAST_SALE_AMT", "prop_last_sale_amt") \
       .withColumnRenamed("PROP_ATTR_BR_CNT", "prop_attr_br_cnt") \
       .withColumnRenamed("PROP_ATTR_BTH_CNT", "prop_attr_bth_cnt") \
       .withColumnRenamed("PROP_ATTR_SQFT_NR", "prop_attr_sqft_nr") \
       .withColumnRenamed("PROP_YR_BLT_NR", "prop_yr_blt_nr") \
       .withColumnRenamed("PROP_ADDRESS_LINE_TXT", "prop_address_line_txt") \
       .withColumnRenamed("PROP_CITY_NM", "prop_city_nm") \
       .withColumnRenamed("PROP_STATE_NM", "prop_state_nm") \
       .withColumnRenamed("PROP_CNTY_NM", "prop_cnty_nm") \
       .withColumnRenamed("PROP_ZIP_CD", "prop_zip_cd") \
       .withColumnRenamed("PROP_TLT_CND_NM", "prop_tlt_cnd_nm") \
       .withColumnRenamed("PROP_INT_CND_NM", "prop_int_cnd_nm") \
       .withColumnRenamed("PROP_EXT_CND_NM", "prop_ext_cnd_nm") \
       .withColumnRenamed("PROP_BTH_CND_NM", "prop_bth_cnd_nm") \
       .withColumnRenamed("PROP_KTH_CND_NM", "prop_kth_cnd_nm") \
       .withColumnRenamed("PROP_LIST_PRICE_AMT", "prop_list_price_amt")


# COMMAND ----------


etl_nr = etl_nr + 1 
df = df.drop("load_date_dt","etl_nr","brnz_prps_prop_sales_dtl_sk","etl_recorded_gmts","record_inserted_ts").withColumn("load_date_dt",current_date()).withColumn("etl_nr",lit(etl_nr).cast('bigint')).withColumn("brnz_prps_prop_sales_dtl_sk",lit(brnz_prps_prop_sales_dtl_sk)+ monotonically_increasing_id()).withColumn("etl_recorded_gmts",current_timestamp()).withColumn("record_inserted_ts",current_timestamp()).withColumn("active_rec_ind",lit(None)).withColumn("year",year(col("prop_last_sale_dt")))


# COMMAND ----------

df = df.withColumn("brnz_prps_prop_sales_dtl_sk", col("brnz_prps_prop_sales_dtl_sk").cast("bigint")) \
       .withColumn("load_date_dt", col("load_date_dt").cast("date")) \
       .withColumn("etl_nr", col("etl_nr").cast("bigint")) \
       .withColumn("etl_recorded_gmts", col("etl_recorded_gmts").cast("timestamp")) \
       .withColumn("record_inserted_ts", col("record_inserted_ts").cast("timestamp")) \
       .withColumn("investor_company_nm_txt", col("investor_company_nm_txt").cast("string")) \
       .withColumn("prop_last_sale_dt", col("prop_last_sale_dt").cast("date")) \
       .withColumn("prop_last_sale_amt", col("prop_last_sale_amt").cast("double")) \
       .withColumn("prop_attr_br_cnt", col("prop_attr_br_cnt").cast("int")) \
       .withColumn("prop_attr_bth_cnt", col("prop_attr_bth_cnt").cast("int")) \
       .withColumn("prop_attr_sqft_nr", col("prop_attr_sqft_nr").cast("int")) \
       .withColumn("prop_yr_blt_nr", col("prop_yr_blt_nr").cast("int")) \
       .withColumn("prop_address_line_txt", col("prop_address_line_txt").cast("string")) \
       .withColumn("prop_city_nm", col("prop_city_nm").cast("string")) \
       .withColumn("prop_state_nm", col("prop_state_nm").cast("string")) \
       .withColumn("prop_cnty_nm", col("prop_cnty_nm").cast("string")) \
       .withColumn("prop_zip_cd", col("prop_zip_cd").cast("string")) \
       .withColumn("prop_tlt_cnd_nm", col("prop_tlt_cnd_nm").cast("string")) \
       .withColumn("prop_int_cnd_nm", col("prop_int_cnd_nm").cast("string")) \
       .withColumn("prop_ext_cnd_nm", col("prop_ext_cnd_nm").cast("string")) \
       .withColumn("prop_bth_cnd_nm", col("prop_bth_cnd_nm").cast("string")) \
       .withColumn("prop_kth_cnd_nm", col("prop_kth_cnd_nm").cast("string")) \
       .withColumn("prop_list_price_amt", col("prop_list_price_amt").cast("double")) \
       .withColumn("active_rec_ind", col("active_rec_ind").cast("boolean")) \
       .withColumn("year", col("year").cast("int")).drop('APN',
        'Est. Equity',
        'Est. Value',
        'Gross Yield %',
        'Lot Size Sqft',
        'Mailing Zip',
        'Monthly Rent')


# COMMAND ----------

df.write.format("delta").mode("append").option("overwriteSchema", "true").partitionBy("investor_company_nm_txt","year").saveAsTable("rehouzd_db.bronze.brnz_prps_prop_sales_dtl")