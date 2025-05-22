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
