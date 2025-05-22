# Databricks notebook source
# catalog_name = "rehouzd_db"
# spark.sql(f"USE CATALOG {catalog_name}")
# print(f"Catalog: {catalog_name}")

# # Get all schemas (databases) in the catalog
# schemas = [row.name for row in spark.catalog.listDatabases()]

# for schema in schemas:
#     print(f"\n  Schema: {schema}")
#     spark.sql(f"USE {catalog_name}.{schema}")
        
#     # Tables and columns
#     tables = spark.catalog.listTables(schema)
#     for table in tables:
#         print(f"    Table: {table.name} ({table.tableType})")
#         columns = spark.catalog.listColumns(table.name)
#         for col in columns:
#             print(f"      Column: {col.name} ({col.dataType})")
        
#     # Volumes
#     try:
#         volumes = spark.sql(f"SHOW VOLUMES IN {catalog_name}.{schema}").collect()
#         for v in volumes:
#             print(f"    Volume: {v['volume_name']}")
#     except Exception as e:
#         print(f"    Could not retrieve volumes for schema {schema}: {e}")

# COMMAND ----------

# from pyspark.sql.types import *

# catalog_name = "rehouzd_db"
# spark.sql(f"USE CATALOG {catalog_name}")
# print(f"Catalog: {catalog_name}")

# tables = {
#     catalog_name:{}
# }

# schemas = [row.name for row in spark.catalog.listDatabases()]
# for schema in schemas:
#     print(f"\n  Schema: {schema}")
#     spark.sql(f"USE {catalog_name}.{schema}")
#     if schema not in tables[catalog_name].keys():
#         tables[catalog_name][schema] = {}


#     # Get all tables in the schema
#     try:
#         table_list = spark.catalog.listTables(schema)
#         for table in table_list:
#             full_table_name = f"{schema}.{table.name}"
#             print(f"    Table: {full_table_name} ({table.tableType})")

#             # Only process MANAGED or EXTERNAL tables
#             if table.tableType.upper() in ["MANAGED", "EXTERNAL"]:
#                 try:
#                     columns = spark.catalog.listColumns(table.name)
#                     struct_fields = []
#                     for col in columns:
#                         dt = col.dataType.lower()
#                         nullable = col.nullable

#                         # Map to Spark data types
#                         spark_type = {
#                             "string": StringType(),
#                             "int": IntegerType(),
#                             "integer": IntegerType(),
#                             "bigint": LongType(),
#                             "long": LongType(),
#                             "double": DoubleType(),
#                             "float": FloatType(),
#                             "boolean": BooleanType(),
#                             "date": DateType(),
#                             "timestamp": TimestampType(),
#                             "binary": BinaryType(),
#                         }.get(dt, StringType())  # Default to StringType for unknowns

#                         struct_fields.append(StructField(col.name, spark_type, nullable))

#                     tables[catalog_name][schema][full_table_name] = StructType(struct_fields)
#                 except Exception as e:
#                     print(f"      Could not retrieve columns for table {full_table_name}: {e}")
#     except Exception as e:
#         print(f"    Could not retrieve tables for schema {schema}: {e}")

#     # Optionally get volumes
#     try:
#         volumes = spark.sql(f"SHOW VOLUMES IN {catalog_name}.{schema}").collect()
#         for v in volumes:
#             print(f"    Volume: {v['volume_name']}")
#     except Exception as e:
#         print(f"    Could not retrieve volumes for schema {schema}: {e}")

# # Print the result as Python code
# print("\n\n# Final table schemas:")
# for tbl, struct in tables.items():
#     print(f'"{tbl}": {struct},\n')


# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

dic = {'rehouzd_db': {'bronze': {'bronze.brnz_goog_prop_add_dtl': StructType([StructField('brnz_goog_prop_address_dtl_sk', LongType(), True), StructField('load_date_dt', DateType(), True), StructField('etl_nr', LongType(), True), StructField('etl_recorded_gmts', TimestampType(), True), StructField('record_inserted_ts', TimestampType(), True), StructField('prop_address_line_txt', StringType(), True), StructField('prop_city_nm', StringType(), True), StructField('prop_state_nm', StringType(), True), StructField('prop_cnty_nm', StringType(), True), StructField('prop_zip_cd', StringType(), True)]),
   'bronze.brnz_prcl_prop_sales_dtl': StructType([StructField('brnz_prcl_prop_sales_dtl_sk', LongType(), True), StructField('load_date_dt', DateType(), True), StructField('etl_nr', LongType(), True), StructField('etl_recorded_gmts', TimestampType(), True), StructField('record_inserted_ts', TimestampType(), True), StructField('investor_company_nm_txt', StringType(), True), StructField('prop_sale_dt', DateType(), True), StructField('prop_sale_amt', DoubleType(), True), StructField('prop_attr_br_cnt', IntegerType(), True), StructField('prop_attr_bth_cnt', IntegerType(), True), StructField('prop_attr_sqft_nr', IntegerType(), True), StructField('prop_yr_blt_nr', IntegerType(), True), StructField('prop_address_line_txt', StringType(), True), StructField('prop_city_nm', StringType(), True), StructField('prop_state_nm', StringType(), True), StructField('prop_cnty_nm', StringType(), True), StructField('prop_zip_cd', StringType(), True), StructField('prop_list_price_amt', StringType(), True), StructField('prop_acty_status_cd', StringType(), True), StructField('prop_acty_status_dc', StringType(), True), StructField('prop_acty_sub_status_cd', StringType(), True), StructField('prop_acty_sub_status_dc', StringType(), True), StructField('prop_latitude_val', StringType(), True), StructField('prop_longitude_val', StringType(), True)]),
   'bronze.brnz_prps_prop_sales_db_rec': StructType([StructField('brnz_prps_prop_sales_dtl_sk', LongType(), True), StructField('load_date_dt', DateType(), True), StructField('etl_nr', LongType(), True), StructField('etl_recorded_gmts', TimestampType(), True), StructField('record_inserted_ts', TimestampType(), True), StructField('investor_company_nm_txt', StringType(), True), StructField('prop_last_sale_dt', DateType(), True), StructField('prop_last_sale_amt', DoubleType(), True), StructField('prop_attr_br_cnt', IntegerType(), True), StructField('prop_attr_bth_cnt', IntegerType(), True), StructField('prop_attr_sqft_nr', IntegerType(), True), StructField('prop_yr_blt_nr', IntegerType(), True), StructField('prop_address_line_txt', StringType(), True), StructField('prop_city_nm', StringType(), True), StructField('prop_state_nm', StringType(), True), StructField('prop_cnty_nm', StringType(), True), StructField('prop_zip_cd', StringType(), True), StructField('prop_tlt_cnd_nm', StringType(), True), StructField('prop_int_cnd_nm', StringType(), True), StructField('prop_ext_cnd_nm', StringType(), True), StructField('prop_bth_cnd_nm', StringType(), True), StructField('prop_kth_cnd_nm', StringType(), True), StructField('prop_list_price_amt', DoubleType(), True), StructField('active_rec_ind', BooleanType(), True), StructField('year', IntegerType(), True), StructField('address_key', StringType(), True)]),
   'bronze.brnz_prps_prop_sales_dtl': StructType([StructField('brnz_prps_prop_sales_dtl_sk', LongType(), True), StructField('load_date_dt', DateType(), True), StructField('etl_nr', LongType(), True), StructField('etl_recorded_gmts', TimestampType(), True), StructField('record_inserted_ts', TimestampType(), True), StructField('investor_company_nm_txt', StringType(), True), StructField('prop_last_sale_dt', DateType(), True), StructField('prop_last_sale_amt', DoubleType(), True), StructField('prop_attr_br_cnt', IntegerType(), True), StructField('prop_attr_bth_cnt', IntegerType(), True), StructField('prop_attr_sqft_nr', IntegerType(), True), StructField('prop_yr_blt_nr', IntegerType(), True), StructField('prop_address_line_txt', StringType(), True), StructField('prop_city_nm', StringType(), True), StructField('prop_state_nm', StringType(), True), StructField('prop_cnty_nm', StringType(), True), StructField('prop_zip_cd', StringType(), True), StructField('prop_tlt_cnd_nm', StringType(), True), StructField('prop_int_cnd_nm', StringType(), True), StructField('prop_ext_cnd_nm', StringType(), True), StructField('prop_bth_cnd_nm', StringType(), True), StructField('prop_kth_cnd_nm', StringType(), True), StructField('prop_list_price_amt', DoubleType(), True), StructField('active_rec_ind', BooleanType(), True), StructField('year', IntegerType(), True)])},
  'silver': {'silver.slv_prps_prop_sales_dtl': StructType([StructField('slv_prps_prop_pk', LongType(), True), StructField('load_date_dt', DateType(), True), StructField('record_inserted_ts', TimestampType(), True), StructField('investor_company_nm_txt', StringType(), True), StructField('prop_last_sale_dt', DateType(), True), StructField('prop_last_sale_amt', DoubleType(), True), StructField('prop_attr_br_cnt', IntegerType(), True), StructField('prop_attr_bth_cnt', IntegerType(), True), StructField('prop_attr_sqft_nr', IntegerType(), True), StructField('prop_yr_blt_nr', IntegerType(), True), StructField('prop_address_line_txt', StringType(), True), StructField('prop_city_nm', StringType(), True), StructField('prop_state_nm', StringType(), True), StructField('prop_cnty_nm', StringType(), True), StructField('prop_zip_cd', StringType(), True), StructField('prop_tlt_cnd_nm', StringType(), True), StructField('prop_int_cnd_nm', StringType(), True), StructField('prop_ext_cnd_nm', StringType(), True), StructField('prop_bth_cnd_nm', StringType(), True), StructField('prop_kth_cnd_nm', StringType(), True), StructField('prop_list_price_amt', DoubleType(), True), StructField('active_rec_ind', BooleanType(), True), StructField('year', IntegerType(), True)]),
   'silver.slvr_int_inv_dtl': StructType([StructField('investor_company_nm_txt', StringType(), True), StructField('num_prop_purchased_lst_12_mths_nr', LongType(), True), StructField('active_flg', BooleanType(), True), StructField('investor_profile', StringType(), True), StructField('active_rec_ind', BooleanType(), True), StructField('start_timestamp', TimestampType(), True), StructField('end_timestamp', TimestampType(), True), StructField('load_date_dt', DateType(), True), StructField('slvr_int_inv_dtl_sk', LongType(), True)]),
   'silver.slvr_int_inv_dtl_profile': StructType([StructField('investor_company_nm_txt', StringType(), True), StructField('num_properties', LongType(), True), StructField('active_flag', LongType(), True), StructField('min_prop_attr_br_cnt', IntegerType(), True), StructField('min_prop_attr_bth_cnt', LongType(), True), StructField('mx_props_amnt', DoubleType(), True), StructField('min_props_amnt', DoubleType(), True), StructField('min_years', LongType(), True), StructField('min_sqft', LongType(), True), StructField('list_zips', StringType(), True), StructField('good_prop_tlt_cnd_nm', LongType(), True), StructField('average_prop_tlt_cnd_nm', LongType(), True), StructField('disrepair_prop_tlt_cnd_nm', LongType(), True), StructField('excellent_prop_tlt_cnd_nm', LongType(), True), StructField('poor_prop_tlt_cnd_nm', LongType(), True), StructField('good_prop_int_cnd_nm', LongType(), True), StructField('average_prop_int_cnd_nm', LongType(), True), StructField('disrepair_prop_int_cnd_nm', LongType(), True), StructField('excellent_prop_int_cnd_nm', LongType(), True), StructField('poor_prop_int_cnd_nm', LongType(), True), StructField('good_prop_ext_cnd_nm', LongType(), True), StructField('average_prop_ext_cnd_nm', LongType(), True), StructField('disrepair_prop_ext_cnd_nm', LongType(), True), StructField('excellent_prop_ext_cnd_nm', LongType(), True), StructField('poor_prop_ext_cnd_nm', LongType(), True), StructField('good_prop_bth_cnd_nm', LongType(), True), StructField('average_prop_bth_cnd_nm', LongType(), True), StructField('disrepair_prop_bth_cnd_nm', LongType(), True), StructField('excellent_prop_bth_cnd_nm', LongType(), True), StructField('poor_prop_bth_cnd_nm', LongType(), True), StructField('good_prop_kth_cnd_nm', LongType(), True), StructField('average_prop_kth_cnd_nm', LongType(), True), StructField('disrepair_prop_kth_cnd_nm', LongType(), True), StructField('excellent_prop_kth_cnd_nm', LongType(), True), StructField('poor_prop_kth_cnd_nm', LongType(), True), StructField('slvr_prps_prop_sales_dtl_sk', LongType(), True), StructField('etl_nr', LongType(), True), StructField('load_date_dt', DateType(), True), StructField('etl_recorded_gmts', TimestampType(), True), StructField('record_inserted_ts', TimestampType(), True), StructField('year', IntegerType(), True)]),
   'silver.slvr_int_prop': StructType([StructField('brnz_prcl_prop_sales_dtl_sk', LongType(), True), StructField('slvr_int_inv_dtl_sk', LongType(), True), StructField('prop_attr_br_cnt', IntegerType(), True), StructField('prop_attr_bth_cnt', IntegerType(), True), StructField('prop_attr_sqft_nr', IntegerType(), True), StructField('prop_yr_blt_nr', IntegerType(), True), StructField('prop_zip_cd', StringType(), True), StructField('prop_city_nm', StringType(), True), StructField('prop_state_nm', StringType(), True), StructField('prop_address_line_txt', StringType(), True), StructField('src_system_cd', StringType(), True), StructField('src_system_dc', StringType(), True), StructField('slvr_int_prop_sk', LongType(), True), StructField('load_date_dt', DateType(), True), StructField('record_inserted_ts', TimestampType(), True), StructField('etl_nr', IntegerType(), True)]),
   'silver.slvr_int_prop_comps': StructType([StructField('prop_attr_br_cnt', IntegerType(), True), StructField('slvr_int_prop_sk', LongType(), True), StructField('brnz_prcl_prop_sales_dtl_sk', LongType(), True), StructField('prop_attr_bth_cnt', IntegerType(), True), StructField('prop_attr_sqft_nr', IntegerType(), True), StructField('prop_yr_blt_nr', IntegerType(), True), StructField('prop_address_line_txt', StringType(), True), StructField('prop_city_nm', StringType(), True), StructField('prop_state_nm', StringType(), True), StructField('prop_cnty_nm', StringType(), True), StructField('prop_zip_cd', StringType(), True), StructField('prop_latitude_val', StringType(), True), StructField('prop_longitude_val', StringType(), True), StructField('prop_acty_status_cd', StringType(), True), StructField('prop_sale_amt', DoubleType(), True), StructField('prop_latest_rental_amt', DoubleType(), True), StructField('prop_latest_sales_amt', DoubleType(), True), StructField('etl_nr', IntegerType(), True), StructField('slvr_int_prop_comps_sk', LongType(), True), StructField('load_date_dt', DateType(), True), StructField('etl_recorded_gmts', TimestampType(), True), StructField('record_inserted_ts', TimestampType(), True)]),
   'silver.slvr_int_prop_sales_dlt': StructType([StructField('slvr_int_prop_dtl_sk', LongType(), True), StructField('slvr_int_prop_fk', IntegerType(), True), StructField('load_date_dt', DateType(), True), StructField('etl_nr', LongType(), True), StructField('etl_recorded_gmts', TimestampType(), True), StructField('record_inserted_ts', TimestampType(), True), StructField('prop_sale_dt', DateType(), True), StructField('prop_sale_amt', DoubleType(), True), StructField('prop_tlt_cnd_nm', StringType(), True), StructField('prop_int_cnd_nm', StringType(), True), StructField('prop_ext_cnd_nm', StringType(), True), StructField('prop_bth_cnd_nm', StringType(), True), StructField('prop_kth_cnd_nm', StringType(), True), StructField('prop_list_price_amt', DoubleType(), True), StructField('latest_record_ind', BooleanType(), True), StructField('usraddr', StringType(), True), StructField('asslvr_int_prop_sk', StringType(), True), StructField('slvr_int_prop_comps_sk', LongType(), True)])}}}

# COMMAND ----------

# MAGIC %md
# MAGIC ### **PLEASE CREATE CATALOG FIRST WITH NAME  - rehouzd_db AND THEN RUN THE SCRIPT TO SETUP THE TABLES AND VOLUMES**

# COMMAND ----------

for schema, tables in dic['rehouzd_db'].items():
    for table_name, schema_struct in tables.items():
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        spark.sql(f"USE SCHEMA {schema}")
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        df = spark.createDataFrame([], schema_struct)
        df.write.saveAsTable(table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC **CREATE A VOLUME CALLED  rehouzd_db.bronze.bronze_volume in the bronze under rehouzd**

# COMMAND ----------

