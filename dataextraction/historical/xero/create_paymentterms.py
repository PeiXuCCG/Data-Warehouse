import pandas as pd
import glob
import numpy as np

# ------------------------------------------
# CONFIG
# ------------------------------------------
company = "ansteys"
field_to_map = "description"
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




u_df = sl[field_to_map].unique()
p_df = pd.DataFrame(u_df, columns=["name"]).dropna()
df = p_df[p_df["name"].str.contains('terms', case=False, na=False)]


OUTPUT_FILE = f"{company}/paymentterms/paymentterms.csv"
df.to_csv(OUTPUT_FILE, index=False)
print(f"Location written to {OUTPUT_FILE}")