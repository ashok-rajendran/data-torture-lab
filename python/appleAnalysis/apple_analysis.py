# Databricks notebook source
# MAGIC %run "./extractor" 

# COMMAND ----------

# MAGIC %run "./transform" 

# COMMAND ----------

# MAGIC %run "./loader"

# COMMAND ----------

class WorkFlow:
    def __init__(self):
        pass

    def runner(self):
        inputDFs = AirpodsAfterIphoneExtractor().extract()
        firstTransformedDf = FirstTransformer().transform(inputDFs)
        AirpodsAfterIphoneLoader(firstTransformedDf).sink()

workFlow = WorkFlow().runner()


# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("AppleAnalysis").getOrCreate()

input_df = spark.read.format("csv").option("header", True).load("/Volumes/workspace/bronze/appleanalysis/Transaction_Updated.csv")

input_df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.airpods_after_iphone