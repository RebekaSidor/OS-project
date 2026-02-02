from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, regexp_replace, when, avg, min, max
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import os

# Έναρξη Spark session
spark = SparkSession.builder.appName("Sudoku Analytics").getOrCreate()

# Φόρτωση Parquet (Πρέπει να έχει δημιουργηθεί ως String schema)
df = spark.read.parquet("/data/sudoku.parquet")

# 1. Υπολογισμός κενών κελιών (Native Spark - 100% ακρίβεια)
df = df.withColumn("empty_cells", 
                   length(col("puzzle")) - length(regexp_replace(col("puzzle"), "0", "")))

# 2. Ταξινόμηση Δυσκολίας
df = df.withColumn("difficulty", 
                   when(col("empty_cells") <= 35, "Easy")
                   .when(col("empty_cells") <= 45, "Medium")
                   .otherwise("Hard"))

# 3. Στατιστικά (Υπολογισμός στη Spark για όλα τα δεδομένα)
stats = df.agg(
    avg("empty_cells").alias("avg"),
    min("empty_cells").alias("min"),
    max("empty_cells").alias("max")
).collect()[0]

total_puzzles = df.count()
avg_val = stats["avg"]
min_val = stats["min"]
max_val = stats["max"]

# 4. Κατανομή Δυσκολίας (GroupBy στη Spark)
diff_counts = df.groupBy("difficulty").count().toPandas()
difficulty_dict = diff_counts.set_index("difficulty")["count"].to_dict()

# Εγγραφή στο TXT
output_path = "/output_spark/results_spark.txt"
os.makedirs(os.path.dirname(output_path), exist_ok=True)
# Εγγραφή στο TXT με σειρά από το μεγαλύτερο στο μικρότερο (όπως το Pandas)
with open(output_path, "w") as f:
    f.write(f"Total Sudoku puzzles: {total_puzzles}\n")
    f.write(f"Average empty cells per puzzle: {avg_val:.2f}\n")
    f.write(f"Minimum empty cells: {min_val}\n")
    f.write(f"Maximum empty cells: {max_val}\n\n")
    f.write("Difficulty distribution:\n")
    
    # Ταξινομούμε το dictionary με βάση τις τιμές (counts) από το μεγαλύτερο στο μικρότερο
    sorted_diff = sorted(difficulty_dict.items(), key=lambda x: x[1], reverse=True)
    
    for key, value in sorted_diff:
        f.write(f"{key}: {value}\n")

# --- ΓΡΑΦΗΜΑΤΑ (ΧΩΡΙΣ SAMPLE) ---

# Plot 1: Difficulty Bar Chart
plt.figure(figsize=(8,6))
plot_order = ["Easy", "Medium", "Hard"]
counts_to_plot = [difficulty_dict.get(k, 0) for k in plot_order]
plt.bar(plot_order, counts_to_plot, color=['#8DCC93', '#FFB152', '#CC4545'])
plt.title("Sudoku Difficulty Distribution (All 9M Puzzles)")
plt.xlabel("Difficulty") # Προστέθηκε για να είναι ίδιο με το Pandas
plt.ylabel("Number of Puzzles")
plt.gca().yaxis.set_major_formatter(FuncFormatter(lambda y, _: f'{int(y):,}'))
plt.tight_layout() # Προστέθηκε για να είναι ίδιο με το Pandas
plt.savefig("/output_spark/difficulty_spark.png")
plt.close()

# Plot 2: Histogram (Binning στη Spark)
hist_data_spark = df.groupBy("empty_cells").count().orderBy("empty_cells").toPandas()
plt.figure(figsize=(10,6))
plt.bar(hist_data_spark["empty_cells"], hist_data_spark["count"], color="#9D4FF7", width=0.8)
plt.title("Distribution of Empty Cells") # Ίδιος τίτλος
plt.xlabel("Number of Empty Cells")
plt.ylabel("Number of Puzzles")
plt.gca().yaxis.set_major_formatter(FuncFormatter(lambda y, _: f'{int(y):,}'))
plt.tight_layout()
plt.savefig("/output_spark/empty_cells_spark.png")
plt.close()

print("Analysis completed successfully on the full dataset.")
spark.stop()