from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder.appName("CSVtoParquet").getOrCreate()

# Ορίζουμε ρητά ότι οι στήλες είναι String για να μην χαθούν ψηφία
schema = StructType([
    StructField("puzzle", StringType(), True),
    StructField("solution", StringType(), True)
])

# Διαβάζουμε το CSV χρησιμοποιώντας το schema
df = spark.read.csv("/data/sudoku.csv", header=True, schema=schema)

# Διαγραφή παλιού και εγγραφή νέου Parquet
df.write.mode("overwrite").parquet("/data/sudoku.parquet")

print("SUCCESS: CSV converted to Parquet with String Schema!")
spark.stop()