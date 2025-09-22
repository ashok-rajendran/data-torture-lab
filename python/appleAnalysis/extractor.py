# Databricks notebook source
# MAGIC %run "./reader_factory"

# COMMAND ----------

class Extractor:
    def __init__(self):
        pass
    def extract(self):
        pass

class SourceDataExtractor(Extractor):
    def extract(self):
        transactionInputDf = get_data_source(
                file_type = "csv",
                file_path = "/Volumes/workspace/bronze/appleanalysis/Transaction_Updated.csv"
            ).get_data_frame()

        customerInputDf = get_data_source(
            file_type = "csv",
            file_path = "/Volumes/workspace/bronze/appleanalysis/Customer_Updated.csv"
        ).get_data_frame()

        productsInputDf = get_data_source(
            file_type = "csv",
            file_path = "/Volumes/workspace/bronze/appleanalysis/Products_Updated.csv"
        ).get_data_frame()

        inputDFs = {
            "transactionInputDf" : transactionInputDf,
            "customerInputDf" : customerInputDf,
            "productsInputDf" : productsInputDf
        }

        return inputDFs