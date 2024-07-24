from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test").getOrCreate()

df = spark.read.format("csv").option("header","true").option("inferSchema","true").load("E:\\0_BIG DATA\\PROJECTS\\iNeuron_Karthik\\A_RAW_DATA\B_DELTA_DATA\\address.csv\\11.2.Delta_Address.csv")
print(df.columns)
for col in df.columns:
    df = df.withColumnRenamed(col, col.lower())

df.show()