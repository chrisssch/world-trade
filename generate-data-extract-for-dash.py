# Initialize
import findspark
findspark.init()

# Imports
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import numpy as np
import pandas as pd

# Configure
conf = SparkConf().setAppName("WorldTrade").setMaster("local[4]")
conf.set("spark.driver.maxResultSize", "2g")
conf.set("spark.driver.memory", "2g")
conf.set("spark.executor.memory", "2g") 
conf.set("spark.executor.pyspark.memory", "2g")

# Initialize
sc = SparkContext(conf=conf)
sqlc = SQLContext(sc)
spark = SparkSession(sc)

# Reporters
schema = StructType([
    StructField("REPORTER", IntegerType(), True),
    StructField("START_DATE", StringType(), True),
    StructField("END_DATE", StringType(), True),
    StructField("REPORTER_NAME", StringType(), True),
    StructField("_c1", StringType(), True),
    StructField("_c2", StringType(), True)
])
reporters = spark.read.csv("data/REPORTERS.txt", sep="\t", header=False, schema=schema)
reporters = reporters \
    .select("REPORTER", "REPORTER_NAME") \
    .withColumn("REPORTER_NAME", F.trim(reporters["REPORTER_NAME"]))

# Partners
schema = StructType([
    StructField("PARTNER", IntegerType(), True),
    StructField("START_DATE", StringType(), True),
    StructField("END_DATE", StringType(), True),
    StructField("PARTNER_NAME", StringType(), True),
    StructField("_c1", StringType(), True),
    StructField("_c2", StringType(), True)
])

partners = spark.read.csv("data/PARTNERS.txt", sep="\t", header=False, schema=schema)
partners = partners.filter(partners["END_DATE"] == "31/12/2500") \
    .withColumn("PARTNER_NAME", F.trim(partners["PARTNER_NAME"])) \
    .select("PARTNER", "PARTNER_NAME")

# Product codes
schema = StructType([
    StructField("PRODUCT_NC", StringType(), True),
    StructField("START_DATE", StringType(), True),
    StructField("END_DATE", StringType(), True),
    StructField("_c1", StringType(), True),
    StructField("PRODUCT_NC_NAME", StringType(), True),
    StructField("_c2", StringType(), True),
    StructField("_c3", StringType(), True)   
])

products = spark.read.csv("data/CN.txt", sep="\t", header=False, schema=schema)
products = products \
    .select("PRODUCT_NC", "PRODUCT_NC_NAME") \
    .withColumn("PRODUCT_NC_NAME", F.trim(products["PRODUCT_NC_NAME"])) \
    .withColumn("HS2", products["PRODUCT_NC"].substr(1, 2))

# Flows
flows = spark.read.parquet("data/parquet/*")
flows = flows.withColumn("YEAR", flows["PERIOD"].substr(1, 4).cast(IntegerType()))

# Combine everything and generate a data extract with
# annual data, 10 years, EU countries, HS2 only
df = flows \
    .select("REPORTER", "PARTNER", "TRADE_TYPE", "PRODUCT_NC", "FLOW", "YEAR", "VALUE_IN_EUROS") \
    .filter(flows["TRADE_TYPE"] == "I") \
    .filter(flows["PRODUCT_NC"] != "TOTAL") \
    .groupBy("YEAR", "REPORTER", "PARTNER", "PRODUCT_NC", "FLOW").agg({"VALUE_IN_EUROS": "sum"}) \
    .groupBy("YEAR", "REPORTER", "PARTNER", "PRODUCT_NC").pivot("FLOW", [1, 2]).sum("sum(VALUE_IN_EUROS)") \
    .withColumnRenamed("1", "IMPORTS").withColumnRenamed("2", "EXPORTS")
df = df \
    .join(F.broadcast(products), "PRODUCT_NC", how="inner") \
    .groupBy("YEAR", "REPORTER", "PARTNER", "HS2").agg({"IMPORTS": "sum", "EXPORTS": "sum"})
df = df \
    .join(F.broadcast(products), df["HS2"] == products["PRODUCT_NC"], how="inner") \
    .withColumnRenamed("PRODUCT_NC_NAME", "HS2_NAME") \
    .withColumn("IMPORTS_MN", df["sum(IMPORTS)"]/1e6).withColumn("EXPORTS_MN", df["sum(EXPORTS)"]/1e6) \
    .select("YEAR", "REPORTER", "PARTNER", df["HS2"], "HS2_NAME", "IMPORTS_MN", "EXPORTS_MN") \
    .join(F.broadcast(reporters), "REPORTER", how="inner") \
    .join(F.broadcast(partners), "PARTNER", how="inner")   
df = df.select("REPORTER", "REPORTER_NAME", "PARTNER", "PARTNER_NAME", "YEAR", "HS2", "HS2_NAME", "IMPORTS_MN", "EXPORTS_MN") \
    .cache()

# Write to csv file
df.coalesce(1).toPandas().to_csv("data_extract_hs2.csv")