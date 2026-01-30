from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CSVtoParquet").getOrCreate()

# Read CSV
df = spark.read.csv("/data/sudoku.csv", header=True, inferSchema=True)

# Write as Parquet
df.write.mode("overwrite").parquet("/data/sudoku.parquet")

print("CSV converted to Parquet successfully!")

spark.stop()
