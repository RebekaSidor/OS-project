from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder.appName("CSVtoParquet").getOrCreate()

# Declare that column type is string so as to not miss out on starting 0's
schema = StructType([
    StructField("puzzle", StringType(), True),
    StructField("solution", StringType(), True)
])

# Read the CSV using schema
df = spark.read.csv("/data/sudoku.csv", header=True, schema=schema)

# Delete old Parquet and create a new one
df.write.mode("overwrite").parquet("/data/sudoku.parquet")

print("SUCCESS: CSV converted to Parquet with String Schema!")
spark.stop()
