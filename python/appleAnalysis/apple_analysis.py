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

