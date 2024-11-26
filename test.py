from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import os
import json
from loguru import logger

def spark_session():
    # print("Creating Spark session...")
    spark = SparkSession.builder.appName("MajorProject").master("local[*]").getOrCreate()
    # print("Spark session created.")
    return spark

def read_json(config_file_path):
    # print(f"Reading JSON config from {config_file_path}...")
    with open(config_file_path, "r") as f:
        config = json.load(f)
    # print("JSON config read successfully.")
    return config

def read_csv(csv_file_path):
    spark = spark_session()
    df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(csv_file_path)
    return df

def total(df, df_column):
    total_no_df = df.na.drop(subset=[df_column])
    total_no_df1 = total_no_df.select(df_column).groupBy(df_column)
    total_no_df2 = total_no_df1.agg(count(df_column).alias("count_"))
    total_no_df3 = total_no_df2.agg(count("*").alias("a")).select("a")
    return total_no_df3.collect()[0][0]

def min_(group_by_column,min_column):
    mini_ = groupBy(group_by_column)\
                .agg(min(min_column).alias("min_"))\
    mini_ = mini_.sort(min_column)
    return mini_

def max_(group_by_column,max_column):
    maxi_ = groupBy(group_by_column)\
                .agg(max(max_column).alias("max_"))
    maxi_ = maxi_.sort(max_column)
    
    return maxi_

def main():
    python_path = "C:/Users/Windows/anaconda3/python.exe"
    os.environ['PYSPARK_PYTHON'] = python_path
    # print("Starting main function...")
    # spark = spark_session()
    config_file_path = "E:\\0_BIG DATA\\PROJECTS\\iNeuron_Karthik\\test.json"
    config_file = read_json(config_file_path)
    file = config_file["config_file"]
    # print(f"Config file path from JSON: {file}")
    csv_file_path = "E:\\0_BIG DATA\\3. PYSPARK(DOCS)\\NOTES\\OfficeData.csv"
    office_df = read_csv(csv_file_path)
    office_df.show()
    columns = []
    for column in office_df.columns:
        columns.append(column)
    
    for column_name in columns:
        logger.info(f"total number of {column_name} is {total(office_df,column_name)}")
    
    office_df_min_max = office_df.withColumn("mini",min_("department","salary"))\
                                    .withColumn("maxi",max_("department","salary"))
    office_df_min_max.show()



if __name__ == "__main__":
    main()
