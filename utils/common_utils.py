from pyspark.sql.functions import *


def get_max_version(spark,tbl_nm):
    try:
        print("get_max_version started")
        mx_version = spark.sql(f"describe history {tbl_nm}").selectExpr("max(version)").collect()[0][0]
        print("get_max_version completed.")
        return mx_version
    except Exception as err:
        print(f"et_max_version got an error  - {err}")
        raise err

def get_max_pk_etl_nr(spark,tbl_nm,pk):
    try:
        etl_nr, max_pk = spark.read.table(tbl_nm).agg(
            coalesce(max("etl_nr"),lit(0)).alias("etl_nr"),
            coalesce(max(pk),lit(0)).alias("max_pk")
        ).collect()[0]
        return etl_nr, max_pk
    except Exception as err:
        print(f"get_max_pk_etl_nr got an error  - {err}")
        raise err
