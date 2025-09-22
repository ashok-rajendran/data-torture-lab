# Databricks notebook source
# MAGIC %run "./extractor" 

# COMMAND ----------

# MAGIC %run "./transform" 

# COMMAND ----------

# MAGIC %run "./loader"

# COMMAND ----------

class AirpodsAfterIphoneWorkFlow:
    """
    ETL to Find Customers who bought Airpods  after buying an Iphone 
    """
    def __init__(self):
        pass

    def runner(self):
        inputDFs = SourceDataExtractor().extract()
        AirpodsAfterIphoneTransformedDf = AirpodsAfterIphoneTransformer().transform(inputDFs)
        AirpodsAfterIphoneLoader(AirpodsAfterIphoneTransformedDf).sink()


# COMMAND ----------

class OnlyAirpodsAndIphoneWorkFlow:
    """
    ETL to Find Customers who bought Only Iphone and Airpods
    """
    def __init__(self):
        pass

    def runner(self):
        inputDFs = SourceDataExtractor().extract()
        OnlyAirpodsAndIphoneTransformedDf = OnlyAirpodsAndIphoneTransformer().transform(inputDFs)
        OnlyAirpodsAndIphoneLoader(OnlyAirpodsAndIphoneTransformedDf).sink()


# COMMAND ----------

class WorkFlowRunner:
    def __init__(self, name):
        self.name = name

    def runner(self):
        if self.name == "AirpodsAfterIphone":
            return AirpodsAfterIphoneWorkFlow().runner()
        elif self.name == "OnlyAirpodsAndIphone":
            return OnlyAirpodsAndIphoneWorkFlow().runner()

name = "AirpodsAfterIphone"
workFlow = WorkFlowRunner(name).runner()
name = "OnlyAirpodsAndIphone"
workFlow = WorkFlowRunner(name).runner()


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.airpods_after_iphone

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.only_airpods_and_iphone