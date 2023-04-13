import os
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, sum

# settings
FILE_PATH = "flights"
FILE_NAME = "flights.csv"

# rereview env variables
{k: v for k, v in os.environ.items() if k.startswith("SPARK")}

# http://localhost:4040/jobs/
spark = SparkSession.builder.appName(f"{FILE_PATH}").enableHiveSupport().getOrCreate()

# read data
df = spark.read.csv(f"{FILE_PATH}/{FILE_NAME}", header=True, inferSchema=True)

df.count()
df.columns
df.count(), len(df.columns)

df.show(3)
df.dtypes
df.printSchema()

df.agg(
    *[sum(col(c).isNull().cast("int")).alias(c + "_null_count") for c in df.columns]
).toPandas().T

# TODO
# unlock basic funcitonalities as insull or describe etc
# work with MLLib
