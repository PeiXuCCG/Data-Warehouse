import pandas as pd
import glob

# Read CSV
company = "countrycaremelbourne"
GL_PATH = f"{company}/glentry/*.csv" 
OUTPUT_FILE = f"{company}/glentry/clean_with_account.csv"

def load_csvs(path):
    files = glob.glob(path)
    dfs = []
    for f in files:
        df = pd.read_csv(f, dtype=str, encoding="latin-1", keep_default_na=False)  # <-- remove header=None
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True)

# Clean 'Type' column: convert to string and strip spaces
# RUN
sl = load_csvs(GL_PATH)

# Normalize 'Type' column: convert to string and strip whitespace
sl["Type"] = sl["Type"].astype(str).str.strip()

# Keep only rows where 'Type' is not blank or null
df_filtered = sl[sl["Type"] != ""]

df_filtered["AccountNumber"] = df_filtered["Sorting Data"].astype(str).str.split("-").str[0]

df_filtered.to_csv(OUTPUT_FILE, index=False)
print("Written:", OUTPUT_FILE)