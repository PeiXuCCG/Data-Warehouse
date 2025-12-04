import pandas as pd
import glob
import numpy as np

# ------------------------------------------
# CONFIG
# ------------------------------------------
company = "lakeside_mobility"
prescriber_list = ['Doctor', 'Occupational', 'Physio']
CUSTOMER_PATH = f"{company}/customer/*.csv"
OUTPUT_FILE = f"{company}/prescriber/prescriber.csv"


def load_csvs(path_glob):
    files = glob.glob(path_glob)
    if not files:
        raise ValueError(f"No CSV files found at {path_glob}")

    print("Files loaded:")
    for f in files:
        print(f)

    return pd.concat([pd.read_csv(f) for f in files], ignore_index=True)


def clean_cols(df):
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "", regex=False)
        .str.replace("/", "", regex=False)
        .str.replace("%", "", regex=False)
    )
    return df


# ------------------------------------------
# LOAD + CLEAN SOURCE
# ------------------------------------------
print("Loading Sales Invoice Detail CSVs...")
sl = load_csvs(CUSTOMER_PATH)
sl = clean_cols(sl)

df = pd.DataFrame(sl)

# ------------------------------------------
# FIND MATCHES IN ANY COLUMN
# ------------------------------------------
# Convert prescriber_list to lowercase for matching
prescriber_list_lower = [p.lower() for p in prescriber_list]

# Create a mask: True if **any** cell in a row contains any prescriber keyword
mask = df.apply(
    lambda row: any(
        any(p in str(cell).lower() for p in prescriber_list_lower)
        for cell in row
    ),
    axis=1
)

# Filter matched rows
p_df = df[mask].copy()

# ------------------------------------------
# WRITE OUTPUT
# ------------------------------------------
p_df.to_csv(OUTPUT_FILE, index=False)
print(f"prescriber written to {OUTPUT_FILE}")
