# Databricks notebook source
# MAGIC %run "./loader_factory" 

# COMMAND ----------

class AbstractLoader:
    def __init__(self, transformedDf):
        self.transformedDf = transformedDf
    def sink(self, transformedDf):
        pass

class AirpodsAfterIphoneLoader(AbstractLoader):
    def sink(self):
        self.transformedDf.write.format("delta").mode("overwrite").saveAsTable(
            "default.airpods_after_iphone"
        )