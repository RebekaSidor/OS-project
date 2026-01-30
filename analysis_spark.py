from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import IntegerType, StringType
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import time

# Start Spark session
spark = SparkSession.builder.appName("Sudoku Analytics").getOrCreate()

#Measure start time load
start_parquet = time.time()

# Load Parquet dataset
df = spark.read.parquet("/data/sudoku.parquet")

end_parquet = time.time()
parquet_time = end_parquet - start_parquet
#-----------------------------------------------------------------------

#Ensure puzzles are strings
df = df.withColumn("puzzle", col("puzzle").cast(StringType()))

#Count empty cells ('0') in each puzzle
count_empty_udf = udf(lambda x: x.count("0"), IntegerType())
df = df.withColumn("empty_cells", count_empty_udf(col("puzzle")))

#Difficulty classification
def difficulty(empty):
    if empty <= 40:
        return "Easy"
    elif empty <= 50:
        return "Medium"
    else:
        return "Hard"

difficulty_udf = udf(difficulty, StringType())
df = df.withColumn("difficulty", difficulty_udf(col("empty_cells")))

#Basic analytics
total_puzzles = df.count()
avg_empty = df.agg({"empty_cells": "avg"}).collect()[0][0]
min_empty = df.agg({"empty_cells": "min"}).collect()[0][0]
max_empty = df.agg({"empty_cells": "max"}).collect()[0][0]

difficulty_counts = df.groupBy("difficulty").count().collect()
difficulty_dict = {row['difficulty']: row['count'] for row in difficulty_counts}

#Write results to txt file
with open("/output/results.txt", "w") as f:
    f.write(f"Total Sudoku puzzles: {total_puzzles}\n")
    f.write(f"Average empty cells per puzzle: {avg_empty:.2f}\n")
    f.write(f"Minimum empty cells: {min_empty}\n")
    f.write(f"Maximum empty cells: {max_empty}\n\n")
    f.write("Difficulty distribution:\n")
    for key in ["Easy", "Medium", "Hard"]:
        f.write(f"{key}: {difficulty_dict.get(key, 0)}\n")

#Plots
#Take sample to Pandas for plotting
sample_df = df.limit(10000).toPandas()

#Plot 1: empty cells distribution
plt.figure(figsize=(8,6))
plt.hist(sample_df["empty_cells"], bins=20, color="#9D4FF7")
plt.title("Distribution of Empty Cells per Sudoku Puzzle")
plt.xlabel("Number of Empty Cells")
plt.ylabel("Number of Puzzles")
formatter = FuncFormatter(lambda y, _: f'{int(y):,}')
plt.gca().yaxis.set_major_formatter(formatter)
plt.tight_layout()
plt.savefig("/output/empty_cells.png")
plt.close()

#Plot 2: difficulty distribution
plt.figure()
sample_df["difficulty"].value_counts().reindex(["Easy","Medium","Hard"]).plot(
    kind="bar", color=['#8DCC93', '#FFB152', '#CC4545']
)
plt.title("Sudoku Difficulty Distribution")
plt.xlabel("Difficulty")
plt.ylabel("Number of Puzzles")
plt.xticks(rotation=0)
plt.ticklabel_format(style='plain', axis='y')
plt.tight_layout()
plt.savefig("/output/difficulty.png")
plt.close()

print("Sudoku analysis completed successfully with Spark.")

spark.stop() #Stop Spark session