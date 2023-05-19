import pandas as pd
import pyspark.sql.functions as F
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import col, sum
from pyspark.sql.types import (
    StringType,
    IntegerType,
    FloatType,
    DoubleType,
    BooleanType,
    DateType,
    TimestampType,
)
from sparkboost import SBFrame

# settings
FILE_PATH = "flights"
FILE_NAME = "flights.csv"

# http://localhost:4040/jobs/
spark = SparkSession.builder.appName(f"{FILE_PATH}").enableHiveSupport().getOrCreate()

# read data
df = spark.read.csv(f"{FILE_PATH}/{FILE_NAME}", header=True, inferSchema=True)
print(f"Shape {df.count(), len(df.columns)}")

# check datatypes
df.printSchema()
df.dtypes

# set datatypes
for col_name in ["FLIGHT_NUMBER"]:
    df = df.withColumn(col_name, col(col_name).cast(StringType()))

# SBFrame usage
sb = SBFrame(df, spark)

sb.head()
sb.isnull()
sb.isnull(normalize=True)
sb.dtypes
sb.printSchema()
