import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

#load dataset
df = pd.read_csv("sudoku.csv")

#count empty cells ('0') in each puzzle
df["empty_cells"] = df["puzzle"].apply(lambda x: x.count("0"))

#difficulty classification
def difficulty(empty):
    if empty <= 40:
        return "Easy"
    elif empty <= 50:
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
with open("results.txt", "w") as f:
    f.write(f"Total Sudoku puzzles: {total_puzzles}\n")
    f.write(f"Average empty cells per puzzle: {avg_empty:.2f}\n")
    f.write(f"Minimum empty cells: {min_empty}\n")
    f.write(f"Maximum empty cells: {max_empty}\n\n")
    f.write("Difficulty distribution:\n")
    f.write(difficulty_counts.to_string())

#plot 1:empty cells
plt.figure(figsize=(8,6))
plt.hist(df["empty_cells"], bins=20, color="#9D4FF7")
plt.title("Distribution of Empty Cells per Sudoku Puzzle")
plt.xlabel("Number of Empty Cells")
plt.ylabel("Number of Puzzles")
formatter = FuncFormatter(lambda y, _: f'{int(y):,}') # Formatter for big numbers
plt.gca().yaxis.set_major_formatter(formatter)
plt.tight_layout()
plt.savefig("empty_cells.png")
plt.close()

#plot 2:difficulty
plt.figure()
difficulty_counts.plot(kind="bar", color=['#8DCC93', '#FFB152', '#CC4545'])
plt.title("Sudoku Difficulty Distribution")
plt.xlabel("Difficulty")
plt.ylabel("Number of Puzzles")
plt.xticks(rotation=0)  # να φαίνονται τα labels ευθεία
plt.ticklabel_format(style='plain', axis='y')  # Δείξε αριθμούς πλήρεις, όχι scientific
plt.tight_layout()
plt.savefig("difficulty.png")
plt.close()

print("Sudoku analysis completed successfully.")