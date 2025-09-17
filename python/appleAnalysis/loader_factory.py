# Databricks notebook source
class DataSink:
    """
    Abstract Class
    """
    def __init__(self, df, path, method, params):
        self.df = df
        self.path = path
        self.method = method
        self.params = params

    def load_data_frame(self):
        """
        Abstract Method, Functions will be defined in sub classes
        """
        raise ValueError("Not Implemented")

class LoadToDataLake(DataSink):
    def load_data_frame(self):
        self.df.write.mode(self.method).save(self.path)

class LoadToDataLakeWithPartition(DataSink):
    def load_data_frame(self):
        partitionByColumns = self.params.get("partitionByColumns")
        self.df.write.mode(self.method.partitionBy(*partitionByColumns)).save(self.path)

def get_sink_source(sink_type, df, path, method, params=None):
    if sink_type == "DataLake":
        return LoadToDataLake(df, path, method, params)
    elif sink_type == "DataLakeWithPartition":
        return LoadToDataLakeWithPartition(df, path, method, params)
    else:
        raise ValueError(f"Not Implemented for sink type: {sink_type}")

