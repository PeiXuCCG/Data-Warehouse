import pandas as pd
import glob
import re
import os

company = "ergo"
INPUT_PATH = f"{company}/purchinvheader"
OUTPUT_FILE = f"{company}/purchinvheader"


def load_csvs(path):
    files = glob.glob(path)
    dfs = []
    for f in files:
        df = pd.read_csv(f, header=None, dtype=str, keep_default_na=False)
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True)


def clean_headers(col):
    col = col.replace(" ", "_")
    col = re.sub(r"[^A-Za-z0-9]", "", col)
    return col


for subdir, dirs, files in os.walk(INPUT_PATH):
    for filename in files:
        if filename.lower().endswith(".csv"):   # process only CSV files
            file_path = os.path.join(subdir, filename)
            print(f"Processing: {file_path}")

            # Read CSV
            df = pd.read_csv(file_path)

            # Clean headers
            df.columns = [clean_headers(c) for c in df.columns]

            base, ext = os.path.splitext(filename)
            new_filename = f"{base}_fixed{ext}"
            new_file_path = os.path.join(subdir, new_filename)

            # Write back in place (same file name, same folder)
            df.to_csv(new_file_path, index=False)


