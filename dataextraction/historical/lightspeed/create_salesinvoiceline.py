import pandas as pd
import glob
import numpy as np

# ------------------------------------------
# CONFIG
# ------------------------------------------
company = "open_mobility"
field_to_map = "linetype"
second_field_to_map = "status"
filter = ["Sale Line"]
second_filter = ["SAVED"]
SALES_PATH = f"{company}/salesinvoiceheader/*.csv"
OUTPUT_FILE = f"{company}/salesinvoiceline/lines.csv"


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


print("Loading Sales CSVs...")
sl = load_csvs(SALES_PATH)



sl = clean_cols(sl)


df = pd.DataFrame(sl)

print(df[field_to_map])

p_df = df[df[field_to_map].isin(filter)]
p_df = p_df[~p_df[second_field_to_map].isin(second_filter)]
p_df["lineno"] = p_df.groupby("receiptnumber").cumcount() + 1


p_df.to_csv(OUTPUT_FILE, index=False)
print(f"file written to {OUTPUT_FILE}")