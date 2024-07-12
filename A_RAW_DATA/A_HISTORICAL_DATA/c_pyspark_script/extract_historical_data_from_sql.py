from pyspark.sql import SparkSession
import json

def read_json(config_file_path):
    with open(config_file_path,"r") as f:
        config = json.load(f)
    return config


def raw_table_names(config, sql_dbase, db_table):
    db_tables = config[sql_dbase][db_table]
    database_tables = list()
    for table_name in db_tables:
        table_name = table_name.lower()
        database_tables.append(table_name)
    return database_tables

def get_spark_session(sql_jar_path):
    
    spark = SparkSession.builder.appName("sql_to_raw_s3")\
                    .config("spark.jars",sql_jar_path)\
                    .master("local[*]")\
                    .getOrCreate()
    return spark

def read_raw_table_from_sql(spark, sql_url,sql_user,sql_password,sql_driver,sql_table_name):
    df = spark.read.format("jdbc").option("url",sql_url)\
                                .option("user",sql_user)\
                                .option("password",sql_password)\
                                .option("driver",sql_driver)\
                                .option("dbtable", sql_table_name).load()
    return df

def write_raw_csv(df, path, file_name):
    df.write.format("csv")\
        .mode("overwrite")\
        .option("header","true")\
        .save(f"{path}\\{file_name}.csv")
    return f"raw data written successfully : {file_name}"

    
def main():
    config_file_path = "E:\\0_BIG DATA\\EDWA_PROJECT\\A_RAW_DATA\\A_HISTORICAL_DATA\\c_pyspark_script\\raw_config.json"
    config = read_json(config_file_path)
    sql_dbase = "sql_dbase"
    db_table = "db_table"
    raw_tables_names = raw_table_names(config,sql_dbase, db_table)
    sql_jar_path = config["sql_jar_path"]
    spark = get_spark_session(sql_jar_path)
    sql_url = config["sql_host"]
    sql_user = config["sql_user"]
    sql_password = config["sql_password"]
    sql_driver = config["sql_driver"]
    write_raw_csv_path = config["write_path"]

    for table_name in raw_tables_names:
        print(table_name)
        # df = read_raw_table_from_sql(spark, sql_url,sql_user,sql_password,sql_driver,table_name)
        # write_raw_csv(df, write_raw_csv_path, table_name)






    spark.stop()

if __name__ == "__main__":
    main()
