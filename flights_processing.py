import pandas as pd
import pyspark.sql.functions as F
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import (
    col,
    count,
    sum,
    mean,
    stddev,
    approxCountDistinct,
    approx_count_distinct,
    countDistinct,
)
from pyspark.sql.types import (
    StringType,
    IntegerType,
    FloatType,
    DoubleType,
    BooleanType,
    DateType,
    TimestampType,
)
from pyspark.sql.window import Window

# settings
FILE_PATH = "flights"
FILE_NAME = "flights.csv"

# http://localhost:4040/jobs/
spark = SparkSession.builder.appName(f"{FILE_PATH}").getOrCreate()

# # what can we do with spark
# [x for x in dir(spark) if not x.startswith("_")]

# read data
# df = spark.read.csv(f"{FILE_PATH}/{FILE_NAME}", header=True, inferSchema=True)

df = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{FILE_PATH}/{FILE_NAME}")
)

# read aux data
airline = (
    spark.read.format("csv")
    .options(header=True, inferSchema=True)
    .load(f"{FILE_PATH}/airlines.csv")
)
airport = (
    spark.read.format("csv")
    .options(header=True, inferSchema=True)
    .load(f"{FILE_PATH}/airports.csv")
)


# Basic functions
print(f"Shape {df.count(), len(df.columns)}")
df.printSchema()
df.schema
df.dtypes
df.show()
df.head()
df.take(2)[0]
type(df.take(2)[0])
df.take(2)[0].YEAR
df.take(2)[0][0]

df.take(1)
airline.count()
airline.show(truncate=False)
airport.count()
airport.show(truncate=False)

# Select columns based on dtypes or names
numeric_cols = [x[0] for x in df.dtypes if x[1] in ["int"]]
categor_cols = [x[0] for x in df.dtypes if x[1] in ["string"]]
time_name_cols = [x[0] for x in df.dtypes if "TIME" in x[0].upper()]

df.select(time_name_cols).head()
df.select(time_name_cols + categor_cols).head()

# Calculate statistics per column
df.select(*[mean(c) for c in time_name_cols]).collect()
df.select(*[stddev(c) for c in time_name_cols]).collect()
df.select(*[approxCountDistinct(c) for c in categor_cols]).collect()
df.select(*[approx_count_distinct(c) for c in categor_cols]).collect()
df.select(*[countDistinct(c) for c in categor_cols]).collect()

# Calculate statistics per column
num_stats = df.agg(
    *[mean(c).alias(f"mean_{c}") for c in time_name_cols]
    + [stddev(c).alias(f"stdev_{c}") for c in time_name_cols]
)

num_stats_calc = num_stats.collect()
print(num_stats_calc)
num_stats_calc[0].asDict()
pd.DataFrame([num_stats_calc[0].asDict()]).T

# Calculate nulls per column
df.select(approx_count_distinct("CANCELLATION_REASON")).collect()

null_stats = df.agg(*[sum(col(c).isNull().cast("int")).alias(c) for c in categor_cols])
null_stats.collect()

# New cols
df = df.withColumn("duration", df.DEPARTURE_TIME / 10)
df.take(1)

# Aggregation & single calc
df.groupBy("AIRLINE").sum("CANCELLED").collect()

# Aggregation & multi calc
g1 = df.groupBy("AIRLINE").agg(
    sum("CANCELLED").alias("sum"),
    count("CANCELLED").alias("count"),
    approx_count_distinct("FLIGHT_NUMBER").alias("distinct_flights"),
)

g1.collect()

# New columns upon aggregation result
g1a = g1.withColumn("RATE", col("sum") / col("count"))
g1a.collect()

# Aggregation & window functions
# Define the window specification
windowSpec = Window.partitionBy("AIRLINE", "YEAR", "MONTH").orderBy("DAY")

# Add a new column with the rolling count
df = df.withColumn("rolling_count", count("AIRLINE").over(windowSpec))

# how to join data
df.join(airline, df.AIRLINE == airline.IATA_CODE, "left").show(5)

# how to use sql
df.createOrReplaceTempView("v1")
airline.createOrReplaceTempView("v2")

spark.sql("SELECT COUNT(*) FROM v1").show()
spark.sql("SELECT * FROM v2").show(truncate=False)

spark.sql(
    "SELECT AIRLINE,COUNT(CANCELLED),SUM(CANCELLED),SUM(CANCELLED)/COUNT(CANCELLED) FROM v1 GROUP BY AIRLINE"
).show()

spark.sql(
    "SELECT v1.FLIGHT_NUMBER, v1.AIRLINE, v2.AIRLINE AS AIRLINE_NAME FROM v1 LEFT JOIN v2 ON v1.AIRLINE=v2.IATA_CODE"
).show()
