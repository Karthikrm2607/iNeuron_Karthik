from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import json
from loguru import logger

def read_json(config_file_path):
    with open (config_file_path, "r") as f:
        config = json.load(f)
    return config

def spark_session():
    spark = SparkSession.builder.appName("sql_to_raw_s3")\
                    .master("local[*]")\
                    .getOrCreate()
    return spark
    
def read_raw_file(spark, raw_file_path, file_name):
    df = spark.read.format("csv")\
                .option("header","true")\
                .option("inferSchema","true")\
                .load(f"{raw_file_path}/{file_name}.csv")
    return df

def subscriber_details(subscriber_df, address_df, city_df,
                        country_df, prepaid_plan_df, postpaid_plan_df):
    subscriber_details_df = subscriber_df.alias("sb")\
        .join(address_df.alias("ad"),col("sb.subscriber_address_id")==col("ad.add_id"),"left")\
        .join(city_df.alias("ct"),col("ad.ct_id")==col("ct.city_id"),"left")\
        .join(country_df.alias("cn"),col("ct.country_id")==col("cn.country_id"),"left")\
        .join(prepaid_plan_df.alias("pp"),col("sb.subscriber_prepaid_plan_id")==col("pp.prepaid_plan_id"),"left")\
        .join(postpaid_plan_df.alias("po"),col("sb.subscriber_postpaid_plan_id")==col("po.postpaid_plan_id"),"left")
        # .drop(col("subscriber_address_id"))\
        # .drop(col("add_id"))\
        # .drop(col("ct_id"))\
        # .drop(col("city_id"))\
        # .drop(col("country_id"))\
        # .drop(col("subscriber_prepaid_plan_id"))\
        # .drop(col("prepaid_plan_id"))\
        # .drop(col("subscriber_postpaid_plan_id"))\
        # .drop(col("postpaid_plan_id"))
    return subscriber_details_df

def select_columns_of_df(subscriber_details_df):
    df = subscriber_details_df.selectExpr("subscriber_id",
                                            "subscriber_name",
                                            "subscriber_mob as Phone_Number",
                                            "subscriber_email","Street as address",
                                            "city_name as city",
                                            "country_name as country",
                                            "subscriber_sys_cre_date as created_date",
                                            "subscriber_sys_upd_date as update_date",
                                            "subscriber_Active_flag as Active_flag",
                                            "prepaid_plan_description",
                                            "prepaid_plan_amount",
                                            "postpaid_plan_description",
                                            "postpaid_plan_amount")
    return df

def write_subscriber_details(df, subscriber_details_save_path, subscriber_details_file_name):
    df.write.format("csv").mode("overwrite")\
        .option("header","true")\
        .save(f"{subscriber_details_save_path}/{subscriber_details_file_name}.csv")


def main():
    spark = spark_session()
    config_file_path = "E:\\0_BIG DATA\\EDWA_PROJECT\\B_TRANSFORMED_DATA\\b_scripts\\transform_config.json"
    config_file_object = read_json(config_file_path)
    raw_file_path = config_file_object["read_raw_file"]
    file_names = config_file_object["file_name"]
    for file in file_names:
        if "country" in file:
            country_df = read_raw_file(spark, raw_file_path, file)
        elif "city" in file:
            city_df = read_raw_file(spark, raw_file_path, file)
        elif "address" in file:
            address_df = read_raw_file(spark, raw_file_path, file)
        elif "staff" in file:
            staff_df = read_raw_file(spark, raw_file_path, file)
        elif "postpaid_plan" in file:
            postpaid_plan_df = read_raw_file(spark, raw_file_path, file)
        elif "prepaid_plan" in file:
            prepaid_plan_df = read_raw_file(spark, raw_file_path, file)
        elif "complaint" in file:
            complaint_df = read_raw_file(spark, raw_file_path, file)
        elif "subscriber" in file:
            subscriber_df = read_raw_file(spark, raw_file_path, file)
        else :
            pass
    

    subscriber_details_df1 = subscriber_details(subscriber_df, address_df, city_df,
                                                country_df, prepaid_plan_df, postpaid_plan_df)

    subscriber_details_df2 = select_columns_of_df(subscriber_details_df1)
    subscriber_details_path = config_file_object["subscriber_details_save_path"]
    subscriber_details_file = config_file_object["subscriber_details_file_name"]

    write_subscriber_details(subscriber_details_df2,
                            subscriber_details_path,
                            subscriber_details_file)
    
    logger.info(f"File written successfully {subscriber_details_file}")

    spark.stop()

if __name__ == "__main__":
    main()
    