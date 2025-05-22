import sys
import os
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from typing import List
from env_variables import *


class DataQuality:
    def __init__(self, tbl_df: DataFrame, rules: List[dict], table_name: str):
        self.tbl_df = tbl_df
        self.rules = rules
        self.table_name = table_name

    def check_data_quality(self,spark,pk_id,mode: str = "filter"):
        try:
            print("Starting data quality checks...")
            all_checks_passed = True
            bad_records = None

            for idx, rule in enumerate(self.rules):
                rule_type = rule.get("rule_type")

                if rule_type == RULE_TYPES["NOT_NULL"]:
                    for col_name in rule.get("columns", []):
                        failed_df = self.tbl_df.filter(col(col_name).isNull()) \
                            .withColumn("rule_type", lit("not_null")) \
                            .withColumn("failed_col_nm", lit(col_name)) \
                            .withColumn("failed_col_value", col(col_name).cast("string") ) \
                            .withColumn("failed_record_detail", lit(f"Null in column {col_name}")) \
                            .withColumn("failed_record", to_json(struct(*[col(c).cast("string").alias(c) for c in self.tbl_df.columns])))

                        bad_records = failed_df if bad_records is None else bad_records.unionByName(failed_df)

                elif rule_type == RULE_TYPES["UNIQUE"]:
                    pk_cols = rule.get("columns", [])
                    dup_df = self.tbl_df.groupBy(pk_cols).count().filter("count > 1").drop("count")
                    failed_df = self.tbl_df.join(dup_df, pk_cols, "inner") \
                        .withColumn("rule_type", lit("unique")) \
                        .withColumn("failed_col_nm", lit(pk_cols)) \
                        .withColumn("failed_col_value", col(pk_cols).cast("string") ) \
                        .withColumn("failed_record_detail", lit(f"Duplicate on {pk_cols}")) \
                        .withColumn("failed_record", to_json(struct(*[col(c).cast("string").alias(c) for c in self.tbl_df.columns])))

                    bad_records = failed_df if bad_records is None else bad_records.unionByName(failed_df)

                elif rule_type == RULE_TYPES["VALUE_RANGE"]:
                    col_name = rule.get("column")
                    min_val = rule.get("min")
                    max_val = rule.get("max")
                    failed_df = self.tbl_df.filter((col(col_name) < min_val) | (col(col_name) > max_val)) \
                        .withColumn("rule_type", lit("value_range")) \
                        .withColumn("failed_col_nm", lit(col_name)) \
                        .withColumn("failed_col_value", col(col_name).cast("string") ) \
                        .withColumn("failed_record_detail", lit(f"Value outside range [{min_val}, {max_val}]")) \
                        .withColumn("failed_record", to_json(struct(*[col(c).cast("string").alias(c) for c in self.tbl_df.columns])))

                    bad_records = failed_df if bad_records is None else bad_records.unionByName(failed_df)

                elif rule_type == RULE_TYPES["ALLOWED_VALUES"]:
                    col_name = rule.get("column")
                    allowed = rule.get("allowed_values", [])
                    failed_df = self.tbl_df.filter(~col(col_name).isin(allowed)) \
                        .withColumn("rule_type", lit("allowed_values")) \
                        .withColumn("failed_col_nm", lit(col_name)) \
                        .withColumn("failed_col_value", col(col_name).cast("string") ) \
                        .withColumn("failed_record_detail", lit(f"Disallowed value in column {col_name}")) \
                        .withColumn("failed_record", to_json(struct(*[col(c).cast("string").alias(c) for c in self.tbl_df.columns])))

                    bad_records = failed_df if bad_records is None else bad_records.unionByName(failed_df)

    
            if bad_records is not None:
                mx_sk = spark.read.table(brnz_bd_rec_tbl).agg(
                    coalesce(max("brnz_bd_rec_sk"),lit(0)).alias("mx_sk")
                ).first()[0]
                mx_sk = mx_sk + 1
                
                if 'bronze.returns' in  self.table_name:
                    bad_records = bad_records \
                        .withColumn("brnz_bd_rec_sk", lit(mx_sk) +  monotonically_increasing_id()) \
                        .withColumn("tbl_nm", lit(self.table_name)) \
                        .withColumn("load_date", current_date()) \
                        .withColumn("load_date_ts", current_timestamp()) \
                        .select(
                            "brnz_bd_rec_sk", "tbl_nm","return_pk",
                            "failed_col_nm", "rule_type", "failed_record_detail","failed_col_value","failed_record",
                            "load_date", "load_date_ts"
                        )
                elif 'bronze.investors' in  self.table_name:
                    bad_records = bad_records \
                        .withColumn("brnz_bd_rec_sk", lit(mx_sk) +  monotonically_increasing_id()) \
                        .withColumn("tbl_nm", lit(self.table_name)) \
                        .withColumn("load_date", current_date()) \
                        .withColumn("load_date_ts", current_timestamp()) \
                        .select(
                            "brnz_bd_rec_sk", "tbl_nm","investor_pk",
                            "failed_col_nm", "rule_type", "failed_record_detail","failed_col_value","failed_record",
                            "load_date", "load_date_ts"
                        )

                elif 'bronze.investment' in  self.table_name:
                    bad_records = bad_records \
                        .withColumn("brnz_bd_rec_sk", lit(mx_sk) +  monotonically_increasing_id()) \
                        .withColumn("tbl_nm", lit(self.table_name)) \
                        .withColumn("load_date", current_date()) \
                        .withColumn("load_date_ts", current_timestamp()) \
                        .select(
                            "brnz_bd_rec_sk", "tbl_nm","investment_pk",
                            "failed_col_nm", "rule_type", "failed_record_detail","failed_col_value","failed_record",
                            "load_date", "load_date_ts"
                        )
                
            display(bad_records)
            if mode == "filter":
                if bad_records is not None:
                    good_df = self.tbl_df.join(bad_records.select(pk_id).distinct(), pk_id, "left_anti")
                    print("Filtered good records returned.")
                    return good_df
                return self.tbl_df

            elif mode == "stat":
                if bad_records is not None:
                    bad_records =  bad_records.drop(pk_id)
                    bad_records.write.mode("append").saveAsTable(brnz_bd_rec_tbl)
                    print("Bad records written to brnz_bad_rec_tbl.")
                return self.tbl_df

            elif mode == "filter_and_stat":
                if bad_records is not None:
                    good_df = self.tbl_df.join(bad_records.select(pk_id).distinct(), pk_id, "left_anti")
                    bad_records =  bad_records.drop(pk_id)
                    bad_records.write.mode("append").saveAsTable(brnz_bd_rec_tbl)
                    print("Bad records written to brnz_bad_rec_tbl.")
                    return good_df
                return self.tbl_df
            else:
                print(f"Unsupported mode: {mode}")
                return self.tbl_df

        except Exception as err:
            print(f"Error occurred in check_data_quality: {str(err)}")
            return self.tbl_df