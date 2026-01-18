import pandas as pd
import glob
import numpy as np

# ------------------------------------------
# CONFIG
# ------------------------------------------
company = "uccello_marketing_eu"
PURCHASE_PATH = f"{company}/purchinvheader/jul25-dec25.csv"
starting_num = 1
starting_order_num = 2648


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


print("Loading Purchase Invoice Header CSVs...")
ph = load_csvs(PURCHASE_PATH)
ph = clean_cols(ph)

# Convert blank/whitespace-only strings to NaN
ph = ph.replace(r"^\s*$", np.nan, regex=True)

# Fill missing reference values with sequential numbers
mask = ph['reference'].isna()
ph.loc[mask, 'reference'] = range(starting_num, mask.sum() + 1)

if 'ordernumber' not in ph.columns:
    ph['ordernumber'] = np.nan   # create a blank column

ph['ordernumber'] = ph['ordernumber'].fillna(
    pd.Series(range(1, ph['ordernumber'].isna().sum() + 1)) + starting_order_num
)



OUTPUT_FILE = f"{company}/purchinvheader/header_withreference_2.csv"
ph.to_csv(OUTPUT_FILE, index=False)
print(f"Location written to {OUTPUT_FILE}")
