import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

from pathlib import Path

# ================== PATHS (portable παντού) ==================
BASE = Path(__file__).parent          # φάκελος που είναι το analysis.py
DATA = BASE / "sudoku.csv"           # το csv δίπλα στο script
OUT = BASE / "output"                # ο φάκελος output δίπλα στο script
OUT.mkdir(exist_ok=True)

#load dataset
df = pd.read_csv(DATA)
#ensure puzzles are strings
df["puzzle"] = df["puzzle"].astype(str)

#count empty cells ('0') in each puzzle
df["empty_cells"] = df["puzzle"].apply(lambda x: x.count("0"))

#difficulty classification
def difficulty(empty):
    if empty <= 35:
        return "Easy"
    elif empty <= 45:
        return "Medium"
    else:
        return "Hard"

df["difficulty"] = df["empty_cells"].apply(difficulty)

#basic analytics
total_puzzles = len(df)
avg_empty = df["empty_cells"].mean()
min_empty = df["empty_cells"].min()
max_empty = df["empty_cells"].max()
difficulty_counts = df["difficulty"].value_counts()


#write results to txt file
with open(OUT / "results.txt", "w") as f:
    f.write(f"Total Sudoku puzzles: {total_puzzles}\n")
    f.write(f"Average empty cells per puzzle: {avg_empty:.2f}\n")
    f.write(f"Minimum empty cells: {min_empty}\n")
    f.write(f"Maximum empty cells: {max_empty}\n\n")
    f.write("Difficulty distribution:\n")
    f.write(difficulty_counts.to_string())

#plot 1:empty cells
hist_data_pd = df["empty_cells"].value_counts().sort_index()
plt.figure(figsize=(10,6))
plt.bar(hist_data_pd.index, hist_data_pd.values, color="#9D4FF7", width=0.8)
plt.title("Distribution of Empty Cells")
plt.xlabel("Number of Empty Cells")
plt.ylabel("Number of Puzzles")
plt.gca().yaxis.set_major_formatter(FuncFormatter(lambda y, _: f'{int(y):,}'))
plt.tight_layout()
plt.savefig(OUT / "empty_cells.png")
plt.close()

#plot 2:difficulty
plt.figure(figsize=(8,6))
plot_order = ["Easy", "Medium", "Hard"]
difficulty_counts = difficulty_counts.reindex(plot_order).fillna(0)
difficulty_counts.plot(kind="bar", color=['#8DCC93', '#FFB152', '#CC4545'])
plt.title("Sudoku Difficulty Distribution")
plt.xlabel("Difficulty")
plt.ylabel("Number of Puzzles")
plt.xticks(rotation=0)
plt.gca().yaxis.set_major_formatter(FuncFormatter(lambda y, _: f'{int(y):,}'))
plt.tight_layout()
plt.savefig(OUT / "difficulty.png")
plt.close()

print("Sudoku analysis completed successfully.")