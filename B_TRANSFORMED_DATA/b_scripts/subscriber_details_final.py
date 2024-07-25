from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
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
    subscriber_details_df1 = subscriber_df.alias("sb")\
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
    subscriber_details_df = subscriber_details_df1.selectExpr("subscriber_id",
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
    return subscriber_details_df

#################### union with delta tables ##################################
def delta_subscriber_details(spark, delta_subscriber_df, delta_address_df, city_df,
                            country_df, prepaid_plan_df, postpaid_plan_df):
    all_df_list = [delta_subscriber_df, 
                    delta_address_df, 
                    city_df,
                    country_df, 
                    prepaid_plan_df, 
                    postpaid_plan_df]
    
    def rename_columns_to_lowercase(df):
        for col_name in df.columns:
            df = df.withColumnRenamed(col_name, col_name.lower())
        return df

    delta_subscriber_df = rename_columns_to_lowercase(delta_subscriber_df)
    delta_address_df = rename_columns_to_lowercase(delta_address_df)
    city_df = rename_columns_to_lowercase(city_df)
    country_df = rename_columns_to_lowercase(country_df)
    prepaid_plan_df = rename_columns_to_lowercase(prepaid_plan_df)
    postpaid_plan_df = rename_columns_to_lowercase(postpaid_plan_df)

    delta_subscriber_df.createOrReplaceTempView("delta_subscriber")
    delta_address_df.createOrReplaceTempView("delta_address")
    city_df.createOrReplaceTempView("city")
    country_df.createOrReplaceTempView("country")
    prepaid_plan_df.createOrReplaceTempView("prepaid_plan")
    postpaid_plan_df.createOrReplaceTempView("postpaid_plan")
    
    delta_subscriber_details_df = spark.sql("""
                                    SELECT
                                        s.sid as subscriber_id,
                                        s.name as subscriber_name,
                                        s.mob as Phone_Number,
                                        s.email as subscriber_email,
                                        a.street as address,
                                        ct.city_name as city,
                                        cn.country_name as country,
                                        s.sys_cre_date as created_date,
                                        s.sys_upd_date as update_date,
                                        s.active_flag as Active_flag,
                                        pp.prepaid_plan_description,
                                        pp.prepaid_plan_amount,
                                        po.postpaid_plan_description,
                                        po.postpaid_plan_amount
                                    FROM
                                        delta_subscriber s,
                                        delta_address a,
                                        city ct,
                                        country cn,
                                        prepaid_plan pp,
                                        postpaid_plan po
                                    WHERE
                                        s.add_id = a.add_id AND
                                        (a.ct_id = ct.city_id OR a.ct_id IS NULL) AND
                                        (ct.country_id = cn.country_id OR ct.country_id IS NULL) AND
                                        (s.prepaid_plan_id = pp.prepaid_plan_id OR s.prepaid_plan_id IS NULL) AND
                                        (s.postpaid_plan_id = po.postpaid_plan_id OR s.postpaid_plan_id IS NULL)
                                    """)


    # delta_subscriber_details_df = df = spark.sql("""
    #                                         SELECT
    #                                             s.sid as subscriber_id,
    #                                             s.name as subscriber_name,
    #                                             s.mob as Phone_Number,
    #                                             s.email as subscriber_email,
    #                                             a.street as address,
    #                                             ct.city_name as city,
    #                                             cn.country_name as country,
    #                                             s.sys_cre_date as created_date,
    #                                             s.sys_upd_date as update_date,
    #                                             s.active_flag as Active_flag,
    #                                             pp.prepaid_plan_description,
    #                                             pp.prepaid_plan_amount,
    #                                             po.postpaid_plan_description,
    #                                             po.postpaid_plan_amount
    #                                         FROM
    #                                             delta_subscriber s
    #                                         LEFT JOIN
    #                                             delta_address a ON s.add_id = a.add_id
    #                                         LEFT JOIN
    #                                             city ct ON a.ct_id = ct.city_id
    #                                         LEFT JOIN
    #                                             country cn ON ct.country_id = cn.country_id
    #                                         LEFT JOIN
    #                                             prepaid_plan pp ON s.prepaid_plan_id = pp.prepaid_plan_id
    #                                         LEFT JOIN
    #                                             postpaid_plan po ON s.postpaid_plan_id = po.postpaid_plan_id
    #                                         """)

    return delta_subscriber_details_df

def union_of_subscriber_details(subscriber_details_df, delta_subscriber_details_df):
    subscriber_details_df_final1 = subscriber_details_df.union(delta_subscriber_details_df)
    subscriber_details_df_final2 = subscriber_details_df_final1\
                                    .withColumn("new_update_date",
                                        when(subscriber_details_df_final1.update_date.isNull(),
                                        to_timestamp(lit("1970-01-01 00:00:00"),format="yyyy-MM-dd HH:mm:SS"))\
                                        .otherwise(subscriber_details_df_final1.update_date))\
                                    .drop("update_date")
    subscriber_details_df_final3 = subscriber_details_df_final2\
                                    .select("*")\
                                    .withColumnRenamed("new_update_date", "update_date")
    subscriber_details_df_final4 = subscriber_details_df_final3\
                                    .withColumn("rn",row_number()\
                                        .over(Window.partitionBy("subscriber_id")\
                                        .orderBy(desc("update_date"))))
    subscriber_details_df_final5 = subscriber_details_df_final4.filter(subscriber_details_df_final4.rn == 1)
    subscriber_details_df_final = subscriber_details_df_final5.drop("rn")
    return subscriber_details_df_final


def write_subscriber_details(df, subscriber_details_save_path, subscriber_details_file_name):
    df.write.format("csv").mode("overwrite")\
        .option("header","true")\
        .save(f"{subscriber_details_save_path}/{subscriber_details_file_name}.csv")


def main():
    spark = spark_session()
    config_file_path = "E:\\0_BIG DATA\\PROJECTS\\iNeuron_Karthik\\B_TRANSFORMED_DATA\\b_scripts\\transform_config.json"
    config_file_object = read_json(config_file_path)

    # for historical data ETL
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
    

    subscriber_details_df = subscriber_details(subscriber_df, address_df, city_df,
                                                country_df, prepaid_plan_df, postpaid_plan_df)
    print("\n ######################printing (subscriber_details_df)###################################\n")
    subscriber_details_df.show(60)

    # for delta load ETL
    raw_file_path = config_file_object["read_raw_file"]
    delta_raw_file_path = config_file_object["delta_subscriber_details_save_path_staging"]
    file_names = config_file_object["file_name"]
    for file in file_names:
        if "address" in file:
            delta_address_df = read_raw_file(spark, delta_raw_file_path, file)
        elif "staff" in file:
            delta_staff_df = read_raw_file(spark, delta_raw_file_path, file)
        elif "complaint" in file:
            delta_complaint_df = read_raw_file(spark, delta_raw_file_path, file)
        elif "subscriber" in file:
            delta_subscriber_df = read_raw_file(spark, delta_raw_file_path, file)
        else :
            pass
    
    delta_subscriber_details_df = delta_subscriber_details(spark, delta_subscriber_df, delta_address_df, city_df,
                            country_df, prepaid_plan_df, postpaid_plan_df)
    print("\n ######################printing (delta_subscriber_details1)###################################\n")
    delta_subscriber_details_df.show(60)

    # union of raw and delta data
    union_of_subscriber_details_file = union_of_subscriber_details(subscriber_details_df, delta_subscriber_details_df)
    print("\n ######################printing (union_of_subscriber_details_file)###################################\n")
    union_of_subscriber_details_file.show(60)

    # saving the final file
    subscriber_details_path = config_file_object["subscriber_details_save_path_final"]
    subscriber_details_file = config_file_object["subscriber_details_file_name"]
    write_subscriber_details(union_of_subscriber_details_file,
                            subscriber_details_path,
                            subscriber_details_file)
    
    logger.info(f"File written successfully {subscriber_details_file}")

    spark.stop()

if __name__ == "__main__":
    main()
    