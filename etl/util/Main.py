from pyspark import F
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
import re

spark = SparkSession.builder.appName("Pyspark-Assignment").getOrCreate()
data =[("Washing Machine","1648770933000",20000,"Samsung","India", 1),\
        ("Refrigerator", "1648770999000", 35000, " LG ", "null", 2),\
        ("Air Cooler","1648770948000",45000," Voltas ", "null", 3)]
schema = StructType([StructField("Product Name",StringType(),True),\
                     StructField("Issue Date",StringType(),True),\
                     StructField("Price",LongType(),True),\
                     StructField("Brand",StringType(),True),\
                     StructField("Country",StringType(),True),\
                     StructField("ProductNumber",StringType(),False)
                     ])
data2 = [(150711, 123456, "EN", 456789, "2021-12-27T08:20:29.842+0000", 1),
         (150439, 234567, "UK", 345678, "2021-12-27T08:21:14.645+0000", 2),
         (150647, 345678, "ES", 234567, "2021-12-27T08:22:42.445+0000", 3)]
schema2 = StructType([StructField("SourceID", LongType(), True),
                      StructField("TransactionNumber", LongType(), True),
                      StructField("Language", StringType(), True),
                      StructField("ModelNumber", LongType(), True),
                      StructField("StartTime", StringType(), True),
                      StructField("ProductNumber", IntegerType(), True)])

# Create dataframe
def CreateDataframe():
    df = spark.createDataFrame(data, schema)
    return df

# Issue date to timestamp format
def Timestamp():
    df2 = spark.createDataFrame(data, schema)
    df2 = df2.withColumn("timestamp", from_unixtime((df2["Issue Date"]/1000)))
    return df2

# Converting time stamp to date type
def DateTime():
    df2 = Timestamp()
    df2 = df2.withColumn("date", date_format(df2["timestamp"], "yyyy-MM-dd"))
    return df2

# Removing white spaces from brand column
def EmptySpaces():
    df3 = spark.createDataFrame(data, schema)
    df3 = df3.withColumn("Brand", trim(df3["Brand"]))
    return df3

# Replacing null value with empty value
def ReplacingEmpty():
    df4 = spark.createDataFrame(data, schema)
    df4 = df4.withColumn("Country", when(col("Country") == "null", "").otherwise("India"))
    return df4

# Creating dataframe
def CreateDataframe2():
    df5 = spark.createDataFrame(data2, schema2)
    return df5

# Camel case to snake snake
def ChangeCamelCase():
    df6 = spark.createDataFrame(data2, schema2)
    def tosnakecase(col_name):
        return re.sub(r"(?<!^)(?=[A-Z])", '_', col_name).lower()

    df6 = df6.toDF(*[tosnakecase(c) for c in df6.columns])
    return df6

# Start time
def StartTime():
    df7 = ChangeCamelCase()
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    def get_milliseconds(time_str):
        return unix_timestamp(time_str, "yyyy-MM-dd'T'HH:mm:ss") * 1000

    df7 = df7.withColumn("start_time_ms", get_milliseconds(df7["start_time"]))
    return df7

# Joining both tables
def Join():
    df = CreateDataframe()
    df5 = CreateDataframe2()
    df8 = df.join(df5, df.ProductNumber == df5.ProductNumber, "full")
    df9 = df8.select(df8["Country"].alias("EN"))        # Selecting "country" as "EN"
    return df9, print(df8.columns, df9.show())


Join()