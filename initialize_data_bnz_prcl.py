# Databricks notebook source
# run_id = dbutils.widgets.get("run_id")
# current_date = dbutils.widgets.get("current_date")
# print(f"Run ID received: {run_id}")
# print(f"current_date received: {current_date}")

run_id = "1223"
current_date = "11/05/2025"

# COMMAND ----------

pip install faker

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from faker import Faker
import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import uuid

fake = Faker()
Faker.seed(42)
random.seed(42)
np.random.seed(42)

# CONFIGURATION
target_size_gb = 25  # or 10 for 10GB
approx_row_size_bytes = 1000  # Estimate ~1KB/row
total_rows = int((target_size_gb * 1024 ** 3) / approx_row_size_bytes)
chunk_size = 1_000_000
num_chunks = total_rows // chunk_size

# Pre-generate a pool of investors
investor_pool = [fake.company() for _ in range(10_000)]

def random_datetime_this_year():
    return fake.date_time_between(start_date="-1y", end_date="now")

def generate_chunk(start_index, n_rows):
    rows = []
    for i in range(n_rows):
        sk = start_index + i
        load_date = datetime.utcnow().date()
        etl_nr = 1
        etl_recorded = datetime.utcnow().isoformat(timespec='milliseconds') + "Z"
        record_inserted = datetime.utcnow().isoformat(timespec='milliseconds') + "Z"
        investor = random.choice(investor_pool) if random.random() > 0.1 else None
        sale_date = fake.date_between(start_date="-2y", end_date="today")
        sale_amt = int(random.uniform(50_000, 1_000_000))
        br = random.randint(1, 6)
        bth = round(random.uniform(1.0, 4.0), 1)
        sqft = random.randint(600, 5000)
        year_built = random.randint(1950, 2023)
        address = fake.street_address().upper()
        city = fake.city().upper()
        state = fake.state_abbr().upper()
        county = f"{fake.city()} County"
        zip_code = fake.zipcode()
        list_price = 0.0 if random.random() > 0.2 else None
        acty_status_cd = "SALE"
        acty_status_dc = None
        acty_sub_status_cd = "SOLD"
        acty_sub_status_dc = None
        lat, lon = fake.latitude(), fake.longitude()

        rows.append([
            sk, load_date, etl_nr, etl_recorded, record_inserted, investor, sale_date,
            sale_amt, br, bth, sqft, year_built, address, city, state, county,
            zip_code, list_price, acty_status_cd, acty_status_dc,
            acty_sub_status_cd, acty_sub_status_dc, lat, lon
        ])

    return pd.DataFrame(rows, columns=[
        "brnz_prcl_prop_sales_dtl_sk", "load_date_dt", "etl_nr", "etl_recorded_gmts", "record_inserted_ts",
        "investor_company_nm_txt", "prop_sale_dt", "prop_sale_amt", "prop_attr_br_cnt", "prop_attr_bth_cnt",
        "prop_attr_sqft_nr", "prop_yr_blt_nr", "prop_address_line_txt", "prop_city_nm", "prop_state_nm",
        "prop_cnty_nm", "prop_zip_cd", "prop_list_price_amt", "prop_acty_status_cd", "prop_acty_status_dc",
        "prop_acty_sub_status_cd", "prop_acty_sub_status_dc", "prop_latitude_val", "prop_longitude_val"
    ])


path_dst = f"/Volumes/rehouzd_db/bronze/bronze_volume/data_rehouzd_mvp_prcl/{current_date}/{run_id}"

try:
    dbutils.fs.ls(path_dst)
except:
    dbutils.fs.mkdirs(path_dst)
    
for chunk_num in range(num_chunks):
    start_idx = chunk_num * chunk_size + 1
    df = generate_chunk(start_idx, chunk_size)
    df.to_csv(f"{path_dst}property_sales_part_{chunk_num+1}.csv", index=False)
    print(f"Saved chunk {chunk_num + 1}/{num_chunks}")


# COMMAND ----------

dbutils.jobs.taskValues.set(key="path_dst", value=path_dst)