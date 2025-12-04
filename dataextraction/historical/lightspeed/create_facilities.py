import pandas as pd
import glob
import numpy as np

# ------------------------------------------
# CONFIG
# ------------------------------------------
company = "ansteys"
field_to_map = "customer_group_name"
prescriber_list = ['Nursing Homes', 'Sporting Clubs']
CUSTOMER_PATH = f"{company}/customer_source/*.csv"


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

print(df[field_to_map])

p_df = df[df[field_to_map].isin(prescriber_list)]

OUTPUT_FILE = f"{company}/facilities/facilities.csv"
p_df.to_csv(OUTPUT_FILE, index=False)
print(f"facilities written to {OUTPUT_FILE}")