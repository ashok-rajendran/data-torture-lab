# Databricks notebook source
from pyspark.sql.window import Window
from pyspark.sql.functions import lead, col, broadcast

class Transformer:
    def __init__(self):
        pass
    def transform(self, inputDf):
        pass

class FirstTransformer(Transformer):
    def transform(self, inputDFs):
        """
        Customers who have bought Airpods after buying the Iphone
        """
        transactionInputDf = inputDFs.get("transactionInputDf")
        print("transactionInputDf in transform")
        transactionInputDf.show()
        windowSpec = Window.partitionBy("customer_id").orderBy("transaction_date")
        transformDF = transactionInputDf.withColumn("nextProductName", lead("product_name").over(windowSpec))
        print("Airpods after buying Iphone")
        transformDF = transformDF.orderBy("customer_id", "transaction_date", "product_name")
        filteredDF = transformDF.filter((col("product_name") == "iPhone") & (col("nextProductName") == "AirPods"))
        customerInputDf = inputDFs.get("customerInputDf")
        productsInputDf = inputDFs.get("productsInputDf")
        joinDF = customerInputDf.join(broadcast(filteredDF),"customer_id")
        joinDF = joinDF.select("customer_id","customer_name","location")
        return joinDF

        
