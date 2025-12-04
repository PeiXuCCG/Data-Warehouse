import pandas as pd
import glob

company = "healthsaver"
SALES_PATH = f"{company}/salesinvoiceline/ITEMSALE.csv"
OUTPUT_FILE = f"{company}/salesinvoiceline/cleaned_2.csv"

REAL_HEADERS = [
    "Item Number", "Invoice No.", "Job", "Date", "Quantity", "Price",
    "Total", "Tax Amount", "Tax Code", "Freight Amount",
    "Freight Tax Amount", "Freight Tax Code", "Amount"
]

NUM_COLS = len(REAL_HEADERS)

def load_csvs(path):
    files = glob.glob(path)
    dfs = []
    for f in files:
        df = pd.read_csv(f, header=0, dtype=str, keep_default_na=False)

        # Trim to required number of columns
        dfs.append(df)

    return pd.concat(dfs, ignore_index=True)

def clean_sales(df):
    cols_to_keep = [c for c in df.columns if c in REAL_HEADERS]
    return df[cols_to_keep]

# RUN
sl = load_csvs(SALES_PATH)


cleaned = clean_sales(sl)




df = cleaned.dropna(how="all")
df = cleaned[~(cleaned.eq("").all(axis=1))]
df.to_csv(OUTPUT_FILE, index=False)

print("Written:", OUTPUT_FILE)
