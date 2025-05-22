from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import *


class XMLReader:
    def __init__(self, spark: SparkSession, file_path: str, row_tag: str):
        self.spark = spark
        self.file_path = file_path
        self.row_tag = row_tag
        self.df = self.load_xml()

    def load_xml(self):
        return self.spark.read \
            .format("com.databricks.spark.xml") \
            .option("rowTag", self.row_tag) \
            .load(self.file_path)

    def get_dataframe(self):
        return self.df
    
    def get_count_rows(self) :
        return self.df.count()
    
    def get_distinct_count_rows(self):
        return self.df.distinct().count()


class CSVReader:
    def __init__(self, spark: SparkSession, file_path: str, header: bool = True, infer_schema: bool = True, delimiter: str = ","):
        self.spark = spark
        self.file_path = file_path
        self.header = header
        self.infer_schema = infer_schema
        self.delimiter = delimiter
        self.df = self.load_csv()

    def load_csv(self) -> DataFrame:
        return self.spark.read \
            .option("header", str(self.header).lower()) \
            .option("inferSchema", str(self.infer_schema).lower()) \
            .option("delimiter", self.delimiter) \
            .csv(self.file_path)

    def get_dataframe(self) -> DataFrame:
        return self.df

    def get_count_rows(self) -> int:
        return self.df.count()

    def get_distinct_count_rows(self) -> int:
        return self.df.distinct().count()


class TableReader:
    def __init__(self, spark: SparkSession, table_name: str):
        self.spark = spark
        self.table_name = table_name
        self.df = self.load_table()

    def load_table(self) -> DataFrame:
        return self.spark.table(self.table_name)

    def get_dataframe(self) -> DataFrame:
        return self.df

    def get_count_rows(self):
        return self.df.count()

    def get_distinct_count_rows(self):
        return self.df.distinct().count()
    def get_schema(self):
        return str(self.df.schema)


class DataIntegrity:
    def __init__(self,spark,csv_file_path,tbl_name,rule_type,xml_file_path = None , row_tag= None):
        self.csv_file_path = csv_file_path
        self.xml_file_path = xml_file_path
        self.tbl_name = tbl_name
        self.rule_types = rule_types
        self.spark = spark
        self.row_tag = row_tag

    def check_intigrity(self,generated_file_schema, existing_tbl):
        try:
            print("Integrity Check: Row count comparison")
            for rule_type in  self.rule_types:
                if not rule_type:
                    print("rule type is not provided")
                    raise Exception("rule type is not provided")
                elif rule_type == "match_row_count_with_xml_csv_table":
                    '''Need to perfrom this check based on the denormalized structure later on thsi xmlf file '''
                    # xmlReader =  XMLReader(self.spark, self.xml_file_path,self.row_tag)
                    csvReader = CSVReader(self.spark, self.csv_file_path)
                    tblReader = TableReader(self.spark, self.tbl_name)

                    if csvReader.get_count_rows() == tblReader.get_count_rows():
                        print("All three data sources have the same number of rows.")
                    else:
                        print("The number of rows in the data sources do not match.")
                        raise Exception("The number of rows in the data sources do not match.")

                elif rule_type == "match_schema_between_existing_table_and_file":
                    if existing_tbl :
                        tblReader = TableReader(self.spark, self.tbl_name)

                        if tblReader.get_schema() == generated_file_schema:
                            print("schema same for the existing table and the generated file.")
                        else:
                            print("schema not same for the existing table and the generated file.")
                            raise Exception("schema not same for the existing table and the generated file.")
        except Exception as err:
            err_data = f"Error in integrity check: {err}"
            print(err_data)
            raise err_data