import pandas as pd
import glob

# Path to folder of CSVs
COMPANY = f"tccg"
PATH = f"{COMPANY}/vendor/*.csv"

# Column to drop
KEY_FIELD = "Key"

# Output file
OUTPUT = f"{COMPANY}/vendor/combined.csv"

# ---- LOAD & APPEND ----
dfs = []

for file in glob.glob(PATH):
    df = pd.read_csv(file, dtype=str, encoding="utf-8")
    dfs.append(df)

combined = pd.concat(dfs, ignore_index=True)

# ---- DROP THE KEY FIELD ----
if KEY_FIELD in combined.columns:
    combined = combined.drop(columns=[KEY_FIELD])

# ---- ADD SEQUENTIAL NUMBER ----
combined["recordid"] = range(1, len(combined) + 1)

# ---- SAVE ----
combined.to_csv(OUTPUT, index=False)

print("Written:", OUTPUT)
