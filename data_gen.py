# Databricks notebook source
pip install faker

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from faker import Faker
import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

fake = Faker()
Faker.seed(42)
random.seed(42)
np.random.seed(42)

# Configuration
total_rows =  27_000_000
chunk_size = 1_000_000  # to handle memory better
num_chunks = total_rows // chunk_size
investor_pool = [fake.company() for _ in range(10_000)]  # smaller reusable pool

def random_date(start, end):
    return fake.date_between(start_date=start, end_date=end)

def generate_chunk(n_rows):
    data = []
    for _ in range(n_rows):
        investor = random.choice(investor_pool)
        address = fake.street_address()
        city = fake.city()
        state = fake.state_abbr()
        zip_code = fake.zipcode()
        county = fake.city()
        apn = fake.uuid4()
        owner_occupied = random.choice(["Yes", "No"])
        mailing_address = fake.street_address()
        mailing_city = fake.city()
        mailing_state = fake.state_abbr()
        mailing_zip = fake.zipcode()
        property_class = random.choice(["Residential", "Commercial"])
        property_type = random.choice(["Single Family", "Multi-Family", "Townhouse"])
        bedrooms = random.randint(1, 6)
        bathrooms = round(random.uniform(1, 4), 1)
        sqft = random.randint(600, 5000)
        year_built = random.randint(1950, 2023)
        lot_size = random.randint(1000, 10000)
        sale_date = random_date("-10y", "today")
        sale_amount = round(random.uniform(50_000, 1_000_000), 2)
        est_value = int(sale_amount * random.uniform(1.0, 1.5))
        est_equity = est_value - sale_amount
        rent = int(est_value * random.uniform(0.005, 0.01))
        yield_percent = round((rent * 12) / est_value * 100, 2)

        data.append([
            investor, address, city, state, zip_code, county, apn, owner_occupied,
            mailing_address, mailing_city, mailing_state, mailing_zip, property_class,
            property_type, bedrooms, bathrooms, sqft, year_built, lot_size,
            sale_date, sale_amount, est_value, est_equity, rent, yield_percent
        ])
    return pd.DataFrame(data, columns=[
        "Investor Name", "Address", "City", "State", "Zip", "County", "APN", "Owner Occupied",
        "Mailing Address", "Mailing City", "Mailing State", "Mailing Zip", "Property Class",
        "Property Type", "Bedrooms", "Bathrooms", "Building Sqft", "Year Built", "Lot Size Sqft",
        "Last Sale Date", "Last Sale Amount", "Est. Value", "Est. Equity", "Monthly Rent", "Gross Yield %"
    ])

# Loop through chunks to write or process
for i in range(num_chunks):
    chunk_df = generate_chunk(chunk_size)
    chunk_df.to_csv(f"/Volumes/rehouzd_db/bronze/bronze_volume/data_rehouzd_mvp_prps/prps/synthetic_investor_data_part_{i+1}.csv", index=False)
    print(f"Chunk {i+1}/{num_chunks} saved.")


# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

df = spark.read \
        .format("xml") \
        .option("rowTag", "Investment") \
        .load("/Volumes/rehouzd_db/bronze/bronze_volume/investment_data.xml")

# Define the schema for Investors (as JSON string inside the column)
investor_schema = StructType([
    StructField("Investor", ArrayType(
        StructType([
            StructField("InvestorID", IntegerType()),
            StructField("Name", StringType()),
            StructField("OwnershipPercentage", DoubleType()),
            StructField("Type", StringType())
        ])
    ))
])

# Define the schema for Returns
return_schema = ArrayType(
    StructType([
        StructField("Amount", DoubleType()),
        StructField("ROI", DoubleType()),
        StructField("Year", IntegerType())
    ])
)

display(df)

# Convert complex types to JSON strings and then parse JSON columns
df_parsed = df.withColumn("investors_json", to_json(col("Investors"))) \
              .withColumn("returns_json", to_json(col("Returns"))) \
              .withColumn("investors_array", from_json(col("investors_json"), investor_schema)) \
              .withColumn("returns_array", from_json(col("returns_json"), return_schema))

display(df_parsed)

# COMMAND ----------

display(df_parsed)

# COMMAND ----------

