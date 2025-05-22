# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

df = spark.read.table("rehouzd_db.silver.slv_prps_prop_sales_dtl").drop("brnz_prps_prop_sales_dtl_sk","load_date_dt","etl_nr","etl_recorded_gmts","record_inserted_ts")
res_df = df.filter((col("prop_last_sale_dt") <= current_date()) & (col("prop_last_sale_dt") >= add_months(current_date(),-12) ) )

sldf = spark.read.table("rehouzd_db.silver.slvr_int_inv_dtl_profile")

slvr_dt = sldf.agg(
    coalesce(max("slvr_prps_prop_sales_dtl_sk"), lit(0) ).alias("slvr_int_inv_dtl_sk"),
    coalesce(max("etl_nr"), lit(0) ).alias("etl_nr")
).collect()[0]

slvr_int_inv_dtl_sk,etl_nr = slvr_dt[0] , slvr_dt[1]
slvr_int_inv_dtl_sk,etl_nr


# COMMAND ----------

rs_df = res_df.groupBy("investor_company_nm_txt").agg(
    count("*").alias("num_properties"),
    sum(when( count("*") >= 2 , 1 ).otherwise(0)).over(Window.partitionBy("investor_company_nm_txt")).alias("active_flag"),
    min("prop_attr_br_cnt").alias("min_prop_attr_br_cnt"),
    floor(min("prop_attr_bth_cnt")).alias("min_prop_attr_bth_cnt"),
    max("prop_last_sale_amt").alias("mx_props_amnt"),
    min("prop_last_sale_amt").alias("min_props_amnt"),
    (floor(min("prop_yr_blt_nr") / 10) * 10).alias("min_years"),
    (floor(min("prop_attr_sqft_nr") / 10) * 10).alias("min_sqft"),
    collect_set("prop_zip_cd").alias("list_zips"),

    # Title condition counts
    sum(when(col("prop_tlt_cnd_nm") == "Good", 1).otherwise(0)).alias("good_prop_tlt_cnd_nm"),
    sum(when(col("prop_tlt_cnd_nm") == "Average", 1).otherwise(0)).alias("average_prop_tlt_cnd_nm"),
    sum(when(col("prop_tlt_cnd_nm") == "Disrepair", 1).otherwise(0)).alias("disrepair_prop_tlt_cnd_nm"),
    sum(when(col("prop_tlt_cnd_nm") == "Excellent", 1).otherwise(0)).alias("excellent_prop_tlt_cnd_nm"),
    sum(when(col("prop_tlt_cnd_nm") == "Poor", 1).otherwise(0)).alias("poor_prop_tlt_cnd_nm"),

    # Interior condition counts
    sum(when(col("prop_int_cnd_nm") == "Good", 1).otherwise(0)).alias("good_prop_int_cnd_nm"),
    sum(when(col("prop_int_cnd_nm") == "Average", 1).otherwise(0)).alias("average_prop_int_cnd_nm"),
    sum(when(col("prop_int_cnd_nm") == "Disrepair", 1).otherwise(0)).alias("disrepair_prop_int_cnd_nm"),
    sum(when(col("prop_int_cnd_nm") == "Excellent", 1).otherwise(0)).alias("excellent_prop_int_cnd_nm"),
    sum(when(col("prop_int_cnd_nm") == "Poor", 1).otherwise(0)).alias("poor_prop_int_cnd_nm"),

    # Exterior condition counts
    sum(when(col("prop_ext_cnd_nm") == "Good", 1).otherwise(0)).alias("good_prop_ext_cnd_nm"),
    sum(when(col("prop_ext_cnd_nm") == "Average", 1).otherwise(0)).alias("average_prop_ext_cnd_nm"),
    sum(when(col("prop_ext_cnd_nm") == "Disrepair", 1).otherwise(0)).alias("disrepair_prop_ext_cnd_nm"),
    sum(when(col("prop_ext_cnd_nm") == "Excellent", 1).otherwise(0)).alias("excellent_prop_ext_cnd_nm"),
    sum(when(col("prop_ext_cnd_nm") == "Poor", 1).otherwise(0)).alias("poor_prop_ext_cnd_nm"),

    # Bathroom condition counts
    sum(when(col("prop_bth_cnd_nm") == "Good", 1).otherwise(0)).alias("good_prop_bth_cnd_nm"),
    sum(when(col("prop_bth_cnd_nm") == "Average", 1).otherwise(0)).alias("average_prop_bth_cnd_nm"),
    sum(when(col("prop_bth_cnd_nm") == "Disrepair", 1).otherwise(0)).alias("disrepair_prop_bth_cnd_nm"),
    sum(when(col("prop_bth_cnd_nm") == "Excellent", 1).otherwise(0)).alias("excellent_prop_bth_cnd_nm"),
    sum(when(col("prop_bth_cnd_nm") == "Poor", 1).otherwise(0)).alias("poor_prop_bth_cnd_nm"),

    # Kitchen condition counts
    sum(when(col("prop_kth_cnd_nm") == "Good", 1).otherwise(0)).alias("good_prop_kth_cnd_nm"),
    sum(when(col("prop_kth_cnd_nm") == "Average", 1).otherwise(0)).alias("average_prop_kth_cnd_nm"),
    sum(when(col("prop_kth_cnd_nm") == "Disrepair", 1).otherwise(0)).alias("disrepair_prop_kth_cnd_nm"),
    sum(when(col("prop_kth_cnd_nm") == "Excellent", 1).otherwise(0)).alias("excellent_prop_kth_cnd_nm"),
    sum(when(col("prop_kth_cnd_nm") == "Poor", 1).otherwise(0)).alias("poor_prop_kth_cnd_nm")
)
slvr_int_inv_dtl_sk,etl_nr = 1 , 1
rs_df = rs_df.withColumn("slvr_prps_prop_sales_dtl_sk",lit(slvr_int_inv_dtl_sk) + monotonically_increasing_id()).withColumn("etl_nr",lit(etl_nr) + monotonically_increasing_id()).withColumn("load_date_dt",current_date()).withColumn("etl_recorded_gmts",current_timestamp()).withColumn("record_inserted_ts",current_timestamp()).withColumn("year",year(current_date()))


# COMMAND ----------

rs_df.write.mode("overwrite").saveAsTable("rehouzd_db.silver.slvr_int_inv_dtl_profile")

# COMMAND ----------

if not rs_df.first():
    rs_df = spark.read.table("rehouzd_db.silver.slvr_int_inv_dtl_profile")

# COMMAND ----------

# import json
# from pyspark.sql.functions import pandas_udf

# def format_json(row):
#     return json.dumps({
#         "min_sqft": row["min_sqft"],
#         "min_year": row["min_years"],
#         "list_zips": eval(row["list_zips"]) if isinstance(row["list_zips"], str) else row["list_zips"],
#         "mx_props_amnt": row["mx_props_amnt"],
#         "min_props_amnt": row["min_props_amnt"],
#         "prop_bth_cnd_nm": {
#             "Good": row["good_prop_bth_cnd_nm"],
#             "Poor": row["poor_prop_bth_cnd_nm"],
#             "Average": row["average_prop_bth_cnd_nm"],
#             "Disrepair": row["disrepair_prop_bth_cnd_nm"],
#             "Excellent": row["excellent_prop_bth_cnd_nm"],
#         },
#         "prop_ext_cnd_nm": {
#             "Good": row["good_prop_ext_cnd_nm"],
#             "Poor": row["poor_prop_ext_cnd_nm"],
#             "Average": row["average_prop_ext_cnd_nm"],
#             "Disrepair": row["disrepair_prop_ext_cnd_nm"],
#             "Excellent": row["excellent_prop_ext_cnd_nm"],
#         }, 
#         "prop_int_cnd_nm": {
#             "Good": row["good_prop_int_cnd_nm"],
#             "Poor": row["poor_prop_int_cnd_nm"],
#             "Average": row["average_prop_int_cnd_nm"],
#             "Disrepair": row["disrepair_prop_int_cnd_nm"],
#             "Excellent": row["excellent_prop_int_cnd_nm"],
#         },
#         "prop_kth_cnd_nm": {
#             "Good": row["good_prop_kth_cnd_nm"],
#             "Poor": row["poor_prop_kth_cnd_nm"],
#             "Average": row["average_prop_kth_cnd_nm"],
#             "Disrepair": row["disrepair_prop_kth_cnd_nm"],
#             "Excellent": row["excellent_prop_kth_cnd_nm"],
#         },
#         "prop_tlt_cnd_nm": {
#             "Good": row["good_prop_tlt_cnd_nm"],
#             "Poor": row["poor_prop_tlt_cnd_nm"],
#             "Average": row["average_prop_tlt_cnd_nm"],
#             "Disrepair": row["disrepair_prop_tlt_cnd_nm"],
#             "Excellent": row["excellent_prop_tlt_cnd_nm"],
#         },
#         "min_prop_attr_br_cnt": row["min_prop_attr_br_cnt"],
#         "min_prop_attr_bth_cnt": row["min_prop_attr_bth_cnt"],
#     })


# COMMAND ----------

# format_json_udf = udf(format_json, StringType())
# # format_json_udf = pandas_udf(format_json, StringType())
# result_df = rs_df.withColumn("formatted_json", format_json_udf(struct(*rs_df.columns)))
# final_df = result_df.select(
#     result_df["investor_company_nm_txt"].alias("investor"),
#     result_df["num_properties"].alias("num_prop_purchased_lst_12_mths_nr"),
#     result_df["active_flag"].alias("active_flg"),
#     result_df["formatted_json"].alias("investor_profile")   
# ).withColumn("active_rec_ind",lit(True)).withColumn("etl_nr",lit(1)).withColumn("etl_reorded_gmts",current_timestamp()).withColumn("record_inserted_ts",current_timestamp()).withColumn("load_date_dt",current_date()).withColumn("slvr_int_inv_dtl_sk",lit(1)+monotonically_increasing_id())

# display(final_df)

# COMMAND ----------

from pyspark.sql.functions import to_json, struct, col, lit, current_timestamp, current_date, monotonically_increasing_id

# First, build the nested structs
formatted_json_col = to_json(struct(
    col("min_sqft"),
    col("min_years").alias("min_year"),
    col("list_zips"),  # Assumes it's already an array, not a stringified list
    col("mx_props_amnt"),
    col("min_props_amnt"),
    struct(
        col("good_prop_bth_cnd_nm").alias("Good"),
        col("poor_prop_bth_cnd_nm").alias("Poor"),
        col("average_prop_bth_cnd_nm").alias("Average"),
        col("disrepair_prop_bth_cnd_nm").alias("Disrepair"),
        col("excellent_prop_bth_cnd_nm").alias("Excellent")
    ).alias("prop_bth_cnd_nm"),
    struct(
        col("good_prop_ext_cnd_nm").alias("Good"),
        col("poor_prop_ext_cnd_nm").alias("Poor"),
        col("average_prop_ext_cnd_nm").alias("Average"),
        col("disrepair_prop_ext_cnd_nm").alias("Disrepair"),
        col("excellent_prop_ext_cnd_nm").alias("Excellent")
    ).alias("prop_ext_cnd_nm"),
    struct(
        col("good_prop_int_cnd_nm").alias("Good"),
        col("poor_prop_int_cnd_nm").alias("Poor"),
        col("average_prop_int_cnd_nm").alias("Average"),
        col("disrepair_prop_int_cnd_nm").alias("Disrepair"),
        col("excellent_prop_int_cnd_nm").alias("Excellent")
    ).alias("prop_int_cnd_nm"),
    struct(
        col("good_prop_kth_cnd_nm").alias("Good"),
        col("poor_prop_kth_cnd_nm").alias("Poor"),
        col("average_prop_kth_cnd_nm").alias("Average"),
        col("disrepair_prop_kth_cnd_nm").alias("Disrepair"),
        col("excellent_prop_kth_cnd_nm").alias("Excellent")
    ).alias("prop_kth_cnd_nm"),
    struct(
        col("good_prop_tlt_cnd_nm").alias("Good"),
        col("poor_prop_tlt_cnd_nm").alias("Poor"),
        col("average_prop_tlt_cnd_nm").alias("Average"),
        col("disrepair_prop_tlt_cnd_nm").alias("Disrepair"),
        col("excellent_prop_tlt_cnd_nm").alias("Excellent")
    ).alias("prop_tlt_cnd_nm"),
    col("min_prop_attr_br_cnt"),
    col("min_prop_attr_bth_cnt")
))

result_df = rs_df.withColumn("investor_profile", formatted_json_col).withColumn("active_flag",when(col("active_flag")==1,lit(True)).otherwise(lit(False)))



# COMMAND ----------

final_df = result_df.select(
    col("investor_company_nm_txt"),
    col("num_properties").alias("num_prop_purchased_lst_12_mths_nr"),
    col("active_flag").alias("active_flg"),
    col("investor_profile")
).withColumn("active_rec_ind", lit(True)) \
 .withColumn("start_timestamp", current_timestamp()) \
 .withColumn("end_timestamp", current_timestamp()) \
 .withColumn("load_date_dt", current_date()) \
 .withColumn("slvr_int_inv_dtl_sk", lit(1) + monotonically_increasing_id())

# COMMAND ----------

# %sql

# MERGE INTO  rehouzd_db.silver.slvr_int_inv_dtl tgt
# USING(
#     select investor_company_nm_txt as merge_key , *
#     from final_df

#     union all

#     select 
#     null as merge_key, final_df.*
#     from final_df
#     join rehouzd_db.silver.slvr_int_inv_dtl dst on 
#     final_df.investor_company_nm_txt = dst.investor_company_nm_txt and 
#     (final_df.num_prop_purchased_lst_12_mths_nr <> dst.num_prop_purchased_lst_12_mths_nr or final_df.investor_profile <> dst.investor_profile) and
#     final_df.active_rec_ind = true 
# ) staged_update
# on tgt.investor_company_nm_txt = staged_update.merge_key
# when matched and (staged_update.num_prop_purchased_lst_12_mths_nr <> tgt.num_prop_purchased_lst_12_mths_nr or staged_update.investor_profile <> tgt.investor_profile) and tgt.active_rec_ind = true then update set tgt.end_timestamp = current_timestamp()  , tgt.active_rec_ind = false
# when not matched then insert (investor_company_nm_txt,num_prop_purchased_lst_12_mths_nr,active_flg,investor_profile,active_rec_ind,start_timestamp,end_timestamp,load_date_dt,slvr_int_inv_dtl_sk)
# values(staged_update.investor_company_nm_txt,staged_update.num_prop_purchased_lst_12_mths_nr,staged_update.active_flg,staged_update.investor_profile,True,current_timestamp(),Null,current_date(),staged_update.slvr_int_inv_dtl_sk)

# COMMAND ----------

from delta.tables import DeltaTable

tgt_df = DeltaTable.forName(spark,'rehouzd_db.silver.slvr_int_inv_dtl')


final_df_with_key = final_df.selectExpr("investor_company_nm_txt as merge_key","*")

staged_df = final_df_with_key.unionAll(
    final_df.alias("final_df").join(
        tgt_df.toDF().alias("dst"),
        [
            col("final_df.investor_company_nm_txt") == col("dst.investor_company_nm_txt"),
            (col("final_df.num_prop_purchased_lst_12_mths_nr") != col("dst.num_prop_purchased_lst_12_mths_nr")) | 
            (col("final_df.investor_profile") != col("dst.investor_profile")),
            col("final_df.active_rec_ind") == True
        ],
        how="inner"
    ).selectExpr("Null as merge_key", "final_df.*")
)


tgt_df.alias("tgt").merge(
    staged_df.alias("staged_df"),
    "tgt.investor_company_nm_txt = staged_df.merge_key"
).whenMatchedUpdate(
    condition = """
        (tgt.num_prop_purchased_lst_12_mths_nr != staged_df.num_prop_purchased_lst_12_mths_nr OR 
         tgt.investor_profile != staged_df.investor_profile)
        AND tgt.active_rec_ind = true
    """,
    set = {
        "end_timestamp": current_timestamp(),
        "active_rec_ind": lit(False)
    }
).whenNotMatchedInsert(
    values = {
        "investor_company_nm_txt": "staged_df.investor_company_nm_txt",
        "num_prop_purchased_lst_12_mths_nr": "staged_df.num_prop_purchased_lst_12_mths_nr",
        "active_flg": "staged_df.active_flg",
        "investor_profile": "staged_df.investor_profile",
        "active_rec_ind": "true",  # as string literal for SQL
        "start_timestamp": "current_timestamp()",
        "end_timestamp": "null",
        "load_date_dt": "current_date()",
        "slvr_int_inv_dtl_sk": "staged_df.slvr_int_inv_dtl_sk"
    }
).execute()
