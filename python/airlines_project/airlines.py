import os
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("AirlinesProject") \
    .getOrCreate()

# Directory where files are stored
input_dir = "/Volumes/workspace/bronze/airline"

# Loop through all files in the directory
for file_name in os.listdir(input_dir):
    if file_name.endswith(".csv"):  
        file_path = os.path.join(input_dir, file_name)

        # Extract table name from file name (before first underscore or dot)
        table_base_name = file_name.replace(".csv", "")
        table_base_name = table_base_name.split("_part_")[0]  # e.g. booking_part_1 → booking
        table_name = f"workspace.bronze.{table_base_name}"       # full table path in catalog

        print(f"Loading file: {file_path} → Table: {table_name}")

        # Read file
        df = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(file_path)

        # Append into respective table
        df.write.format("delta") \
            .mode("append") \
            .saveAsTable(table_name)
