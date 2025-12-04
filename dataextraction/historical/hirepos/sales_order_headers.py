import pandas as pd
import glob
import numpy as np

# ------------------------------------------
# CONFIG
# ------------------------------------------
company = "homecare_equipment"
headers = ['Order Date', 'Order Number', 'Customer', 'Reference', 'Qty', 'Each', 'Total']
CUSTOMER_PATH = f"{company}/salesorder_source/*.csv"
OUTPUT_FILE = f"{company}/salesorderheader/salesorderheader.csv"


def load_csvs(path_glob):
    files = glob.glob(path_glob)
    if not files:
        raise ValueError(f"No CSV files found at {path_glob}")
    for f in files:
        print(f)
        pd.read_csv(f)

    return pd.concat([pd.read_csv(f) for f in files], ignore_index=True)


def clean_cols(df):
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "")
        .str.replace("/", "")
        .str.replace("%", "")
    )
    return df


print("Loading Sales Invoice Detail CSVs...")
sl = load_csvs(CUSTOMER_PATH)



sl = clean_cols(sl)

df = pd.DataFrame(sl)
df.columns = headers




df.to_csv(OUTPUT_FILE, index=False)
print(f"facilities written to {OUTPUT_FILE}")