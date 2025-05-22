# Databricks notebook source
import json
import requests
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime , timedelta
import math
from utils.common_utils import * 
import logging

with open('./config.json') as data:
    datas = json.load(data)

SEARC_ADDR_API = "https://api.parcllabs.com/v1/property/search_address"
EVENT_HISTORY_API = "https://api.parcllabs.com/v1/property/event_history"

# COMMAND ----------

# MAGIC %sql
# MAGIC -- alter table bronze.brnz_goog_prop_add_dtl drop if exists partition(load_date_dt=current_date())
# MAGIC -- alter table bronze.brnz_goog_prop_add_dtl set tblproperties(delta.enableChangeDataFeed = true) 

# COMMAND ----------

def subtract_months(date, months):
    """Subtracts months from a given date without using dateutil."""
    month = date.month - months
    year = date.year

    # Adjust the year and month if necessary
    while month <= 0:
        month += 12
        year -= 1

    # Handle edge cases where day might not exist in target month (e.g., Feb 30)
    dval = (datetime(year, month, 1).replace(day=28) + timedelta(days=4)).day
    day = date.day if date.day <= dval else dval
    # day = min(date.day,dval)
    
    return datetime(year, month, day)

# COMMAND ----------

def get_valid_event(events, primary_type, primary_name, fallback_type=None, fallback_name=None):
    """
    Retrieve the latest event of primary_type and primary_name.
    If its price is 0, fallback to fallback_type and fallback_name.
    """
    primary_event = None
    fallback_event = None
    
    for event in sorted(events, key=lambda x: x["event_date"], reverse=True):
        if event["PROP_ACTY_STATUS_CD"] == primary_type and event["PROP_ACTY_SUB_STATUS_CD"] == primary_name:
            primary_event = event
            if event["price"] > 0:
                return event
        elif fallback_type and fallback_name and event["PROP_ACTY_STATUS_CD"] == fallback_type and event["PROP_ACTY_SUB_STATUS_CD"] == fallback_name:
            fallback_event = event
    
    return primary_event if primary_event and primary_event["price"] > 0 and primary_event["price"] is not None  else fallback_event

# COMMAND ----------

# df1 = spark.read.option("readChangeData", "true").option("startingVersion", 2).table("bronze.brnz_goog_prop_add_dtl").filter(col("_change_type")=='delete').drop("_change_type","_commit_timestamp","_commit_version")
# df2 = spark.read.option("readChangeData", "true").option("startingVersion", 2).table("bronze.brnz_goog_prop_add_dtl").filter(col("_change_type")=='insert').drop("_change_type","_commit_timestamp","_commit_version")
# display(df1.subtract(df2))

# COMMAND ----------

# mx_version = get_max_version(spark,"bronze.brnz_goog_prop_add_dtl")
# ggl_df = spark.read.option("readChangeData", "true").option("startingVersion", mx_version).table("bronze.brnz_goog_prop_add_dtl").select("prop_address_line_txt","prop_city_nm","prop_zip_cd").distinct()
# display(ggl_df)

# COMMAND ----------

float(0)

# COMMAND ----------

def api_info_extract_load_brnx_prc(bnz_pk,etl_nr):
    try:
        from decimal import Decimal, ROUND_HALF_UP
        mx_version = get_max_version(spark,"bronze.brnz_goog_prop_add_dtl")
        ggl_df = spark.read.option("readChangeData", "true").option("startingVersion", mx_version).table("bronze.brnz_goog_prop_add_dtl").select("prop_address_line_txt","prop_city_nm","prop_state_nm","prop_zip_cd").distinct()
        # display(ggl_df)
        rows_to_insert = []

        for row in ggl_df.collect():
            bnz_pk = bnz_pk + 1
            prop_address_line_txt = row[0]
            prop_city_nm = row[1]
            prop_state_nm = row[2]
            prop_zip_cd  = row[3]
            
            headers = {
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
                "Accept": "application/json",
                "Authorization": "40Kajrsnv7zy5ycq0mGFwLMbvNwgpAvekSYgOcCbHAo" 
            }
         
            body = [
                {
                    "address": prop_address_line_txt,
                    "city": prop_city_nm,
                    "state_abbreviation": prop_state_nm,
                    "zip_code": prop_zip_cd
                }
            ]
            srch_res = requests.post(SEARC_ADDR_API, headers=headers, json=body).json()['items']
            load_date_dt = datetime.today().date()
            
            
            for res in srch_res:
                parcl_property_id_lis =res['parcl_property_id'] 
                bedrooms = res['bedrooms']
                bathrooms = int(math.ceil(res['bathrooms']))
                square_footage = res['square_footage']
                year_built =  res['year_built']
                address =  res['address']
                city = res['city']
                state_abbreviation = res['state_abbreviation']
                county = res['county']
                zip_code = res['zip_code']
                PROP_LIST_PRICE_AMT = float(0)
                latitude = Decimal(str(res['latitude'])).quantize(Decimal('0.000001'), rounding=ROUND_HALF_UP)
                longitude = Decimal(str(res['longitude'])).quantize(Decimal('0.000001'), rounding=ROUND_HALF_UP)
                # previous_date = load_date_dt - relativedelta(months=6)
                previous_date = subtract_months(load_date_dt,6)
                previous_date = previous_date.strftime("%Y-%m-%d")
                
                js_bd = {
                    "parcl_property_id": [str(parcl_property_id_lis)],
                    "start_date": previous_date
                }

                event_res = None
                            
                event_res = requests.post(EVENT_HISTORY_API, headers=headers, json=js_bd)
                if event_res.status_code != 200:
                    # print(f"for {parcl_property_id_lis} this is the error not making call - {event_res.json()}")
                    # logging.error(f"for {parcl_property_id_lis} this is the error not making call - {event_res.json()}")
                    continue

                event_res = event_res.json()['items'] 
                PROP_ACTY_STATUS_DC = None 
                PROP_ACTY_SUB_STATUS_DC = None       
                events_list = []
                
                for event in event_res:
                    events_list.append({
                        "PROP_ACTY_STATUS_CD": event['event_type'],
                        "PROP_ACTY_SUB_STATUS_CD": event['event_name'],
                        "event_date": datetime.strptime(event['event_date'], "%Y-%m-%d"),
                        "entity_owner_name": event['entity_owner_name'],
                        "price": event['price']
                    })
              

                # Process SALE - SOLD, fallback to LISTING - PENDING SALE if price is 0
                selected_event = get_valid_event(events_list, "SALE", "SOLD", "LISTING", "PENDING SALE")
                etl_reorded_gmts = datetime.now()
                if selected_event:
                   rows_to_insert.append(
                        [bnz_pk, load_date_dt, etl_nr, etl_reorded_gmts, datetime.now(),
                        selected_event["entity_owner_name"], selected_event["event_date"], float(selected_event["price"]),
                        bedrooms, bathrooms, square_footage, year_built, address, city, state_abbreviation,
                        county, zip_code, PROP_LIST_PRICE_AMT, selected_event["PROP_ACTY_STATUS_CD"],
                        PROP_ACTY_STATUS_DC, selected_event["PROP_ACTY_SUB_STATUS_CD"], PROP_ACTY_SUB_STATUS_DC,
                        latitude, longitude]
                    )

               # Process RENTAL - DELISTED FOR RENT separately
                rental_event = get_valid_event(events_list, "RENTAL", "DELISTED FOR RENT")
                if rental_event:
                    rows_to_insert.append(
                        [bnz_pk, load_date_dt, etl_nr, etl_reorded_gmts, datetime.now(),
                        rental_event["entity_owner_name"], float(rental_event["event_date"]), rental_event["price"],
                        bedrooms, bathrooms, square_footage, year_built, address, city, state_abbreviation,
                        county, zip_code, PROP_LIST_PRICE_AMT, rental_event["PROP_ACTY_STATUS_CD"],
                        PROP_ACTY_STATUS_DC, rental_event["PROP_ACTY_SUB_STATUS_CD"], PROP_ACTY_SUB_STATUS_DC,
                        latitude, longitude]
                    )
    
        columns = StructType([StructField('brnz_prcl_prop_sales_dtl_sk', LongType(), True), StructField('load_date_dt', DateType(), True), StructField('etl_nr', LongType(), True), StructField('etl_recorded_gmts', TimestampType(), True), StructField('record_inserted_ts', TimestampType(), True), StructField('investor_company_nm_txt', StringType(), True), StructField('prop_sale_dt', DateType(), True), StructField('prop_sale_amt', DoubleType(), True), StructField('prop_attr_br_cnt', IntegerType(), True), StructField('prop_attr_bth_cnt', IntegerType(), True), StructField('prop_attr_sqft_nr', IntegerType(), True), StructField('prop_yr_blt_nr', IntegerType(), True), StructField('prop_address_line_txt', StringType(), True), StructField('prop_city_nm', StringType(), True), StructField('prop_state_nm', StringType(), True), StructField('prop_cnty_nm', StringType(), True), StructField('prop_zip_cd', StringType(), True), StructField('prop_list_price_amt', StringType(), True), StructField('prop_acty_status_cd', StringType(), True), StructField('prop_acty_status_dc', StringType(), True), StructField('prop_acty_sub_status_cd', StringType(), True), StructField('prop_acty_sub_status_dc', StringType(), True), StructField('prop_latitude_val', DecimalType(9,6), True), StructField('prop_longitude_val', DecimalType(9,6), True)])
            
        if rows_to_insert is not None:
            print("rows_to_insert is not null.")
            insert_df = spark.createDataFrame(rows_to_insert, schema=columns)
            print("inserting it to the google props table  - ")
            insert_df.write.mode("append").saveAsTable("bronze.brnz_prcl_prop_sales_dtl")
            print("insertion is doen to the google props table  - ")
        else:
            print('rows_to_insert is null :',rows_to_insert)
        return etl_reorded_gmts,etl_nr
    except Exception as err:
        print(err)
        logging.error(f"Error occure in api_info_extract_load_brnx_prc and the error is  - {str(err)}")
        raise Exception (err)


# COMMAND ----------

# from datetime import datetime, timezone
# etl_reorded_gmts = datetime.now(timezone.utc)
# etl_reorded_gmts

# COMMAND ----------

def process_bnz_prcl_dtl():
    try:
        etl_nr ,  max_pk = get_max_pk_etl_nr(spark,"bronze.brnz_prcl_prop_sales_dtl","brnz_prcl_prop_sales_dtl_sk")
        etl_reorded_gmts,etl_nr = api_info_extract_load_brnx_prc(max_pk,etl_nr)
    except Exception as err:
        logging.error(f"Error occure in process_bnz_prcl_dtl and the error is  - {str(err)}")
        raise Exception(err)


# COMMAND ----------

process_bnz_prcl_dtl()

# COMMAND ----------

# df = spark.read.table("bronze.brnz_prcl_prop_sales_dtl")
# df.schema

# COMMAND ----------

# %sql
# select count(*) from bronze.brnz_prcl_prop_sales_dtl

# COMMAND ----------



# COMMAND ----------

