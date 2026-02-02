from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, regexp_replace, when, avg, min, max
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import requests
import json
import os
import time

start_time = time.time()  #start time count

#start spark session
spark = SparkSession.builder.appName("Sudoku Analytics").getOrCreate()

#load Parquet
df = spark.read.parquet("/data/sudoku.parquet")

#count empty cells ('0') in each puzzle
df = df.withColumn("empty_cells", 
                   length(col("puzzle")) - length(regexp_replace(col("puzzle"), "0", "")))

#difficulty classification
df = df.withColumn("difficulty", 
                   when(col("empty_cells") <= 35, "Easy")
                   .when(col("empty_cells") <= 45, "Medium")
                   .otherwise("Hard"))

#basic analytics
stats = df.agg(
    avg("empty_cells").alias("avg"),
    min("empty_cells").alias("min"),
    max("empty_cells").alias("max")
).collect()[0]

total_puzzles = df.count()
avg_val = stats["avg"]
min_val = stats["min"]
max_val = stats["max"]
diff_counts = df.groupBy("difficulty").count().toPandas()
difficulty_dict = diff_counts.set_index("difficulty")["count"].to_dict()

#write results to txt file
output_path = "/output_spark/results_spark.txt"
os.makedirs(os.path.dirname(output_path), exist_ok=True)
with open(output_path, "w") as f:
    f.write(f"Total Sudoku puzzles: {total_puzzles}\n")
    f.write(f"Average empty cells per puzzle: {avg_val:.2f}\n")
    f.write(f"Minimum empty cells: {min_val}\n")
    f.write(f"Maximum empty cells: {max_val}\n\n")
    f.write("Difficulty distribution:\n")
    
    sorted_diff = sorted(difficulty_dict.items(), key=lambda x: x[1], reverse=True)
    
    for key, value in sorted_diff:
        f.write(f"{key}: {value}\n")

#plot 1:difficulty
plt.figure(figsize=(8,6))
plot_order = ["Easy", "Medium", "Hard"]
counts_to_plot = [difficulty_dict.get(k, 0) for k in plot_order]
plt.bar(plot_order, counts_to_plot, color=['#8DCC93', '#FFB152', '#CC4545'])
plt.title("Sudoku Difficulty Distribution (All 9M Puzzles)")
plt.xlabel("Difficulty")
plt.ylabel("Number of Puzzles")
plt.gca().yaxis.set_major_formatter(FuncFormatter(lambda y, _: f'{int(y):,}'))
plt.tight_layout()
plt.savefig("/output_spark/difficulty_spark.png")
plt.close()

#plot 2:empty cells
hist_data_spark = df.groupBy("empty_cells").count().orderBy("empty_cells").toPandas()
plt.figure(figsize=(10,6))
plt.bar(hist_data_spark["empty_cells"], hist_data_spark["count"], color="#9D4FF7", width=0.8)
plt.title("Distribution of Empty Cells")
plt.xlabel("Number of Empty Cells")
plt.ylabel("Number of Puzzles")
plt.gca().yaxis.set_major_formatter(FuncFormatter(lambda y, _: f'{int(y):,}'))
plt.tight_layout()
plt.savefig("/output_spark/empty_cells_spark.png")
plt.close()

end_time = time.time()  #stop time count

print("Analysis completed successfully on the full dataset.")
print(f"Execution time: {end_time - start_time:.2f} seconds")

# LLM ------------------------------------------------------------------

#create output_llm
output_llm_dir = "/output_llm"
os.makedirs(output_llm_dir, exist_ok=True)

#get results from spark
with open("/output_spark/results_spark.txt", "r", encoding="utf-8") as f:
    stats_content = f.read()

#set API from LM Studio
LM_STUDIO_URL = "http://127.0.0.1:1234"

#prompt
prompt = f"""
You are a data analyst summarizing Sudoku statistics.
Data:
{stats_content}

Instructions:
Write a friendly and simple summary of these results for a presentation. 
Explain what the numbers mean for a casual player.
"""

payload = {
    "messages": [
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": "Write a friendly and simple summary of these results"} 
    ],
    "model": "mistralai/mistral-7b-instruct-v0.3",
    "temperature": 0.7,
}

print(f"Sending request to LM Studio... Results will be saved in {output_llm_dir}")

try:
    response = requests.post(LM_STUDIO_URL, json=payload, timeout=60)
    response.raise_for_status()
    
    summary = response.json()['choices'][0]['message']['content']
    
    #save to output_llm
    final_output_path = os.path.join(output_llm_dir, "llm_narrative.txt")
    
    with open(final_output_path, "w", encoding="utf-8") as f:
        f.write("=== PROMPT USED ===\n")
        f.write(prompt)
        f.write("\n\n=== LLM RESPONSE ===\n")
        f.write(summary)

    print(f"Success! The narrative has been saved to: {final_output_path}")
    print("\n--- Summary Preview ---")
    print(summary)

except Exception as e:
    print(f"Error: {e}")
spark.stop()