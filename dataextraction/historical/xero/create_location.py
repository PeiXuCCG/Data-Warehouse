import pandas as pd
import glob
import numpy as np

# ------------------------------------------
# CONFIG
# ------------------------------------------
company = "uccello_marketing_eu"
field_to_map = "grants"
SALES_PATH = f"{company}/salesinvoiceline/*.csv"


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
sl = load_csvs(SALES_PATH)



sl = clean_cols(sl)




df = sl[field_to_map].unique()
p_df = pd.DataFrame(df, columns=["name"]).dropna()


OUTPUT_FILE = f"{company}/location/location.csv"
p_df.to_csv(OUTPUT_FILE, index=False)
print(f"Location written to {OUTPUT_FILE}")