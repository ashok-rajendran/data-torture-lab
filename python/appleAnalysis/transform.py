# Databricks notebook source
from pyspark.sql.window import Window
from pyspark.sql.functions import lead, col, broadcast, collect_set, size, array_contains

class Transformer:
    def __init__(self):
        pass
    def transform(self, inputDf):
        pass


# COMMAND ----------

class AirpodsAfterIphoneTransformer(Transformer):
    def transform(self, inputDFs):
        """
        Customers who have bought Airpods after buying the Iphone
        """
        transactionInputDf = inputDFs.get("transactionInputDf")
        print("Initializing Class : AirpodsAfterIphoneTransformer to get customers who bought Airpods after buying Iphone ")
        windowSpec = Window.partitionBy("customer_id").orderBy("transaction_date")
        transformDF = transactionInputDf.withColumn("nextProductName", lead("product_name").over(windowSpec))
        print("Airpods after buying Iphone")
        transformDF = transformDF.orderBy("customer_id", "transaction_date", "product_name")
        filteredDF = transformDF.filter((col("product_name") == "iPhone") & (col("nextProductName") == "AirPods"))
        customerInputDf = inputDFs.get("customerInputDf")
        joinDF = customerInputDf.join(broadcast(filteredDF),"customer_id")
        joinDF = joinDF.select("customer_id","customer_name","location")
        return joinDF


# COMMAND ----------

class OnlyAirpodsAndIphoneTransformer(Transformer):
    def transform(self, inputDFs):
        """
        Customers who have bought Airpods and Iphone
        """
        transactionInputDf = inputDFs.get("transactionInputDf")
        print("Initializing Class : OnlyAirpodsAndIphoneTransformer to get customers who bought only Airpods and Iphone ")
        groupedDF = transactionInputDf.groupBy("customer_id").agg(collect_set("product_name").alias("products"))
        filteredDF = groupedDF.filter(array_contains(col("products"), "iPhone") & array_contains(col("products"),"AirPods") & (size(col("products")) == 2))
        customerInputDf = inputDFs.get("customerInputDf")
        joinDF = customerInputDf.join(broadcast(filteredDF),"customer_id")
        joinDF = joinDF.select("customer_id","customer_name","location")
        return joinDF
        
        