import pandas as pd
import glob

company = "healthsaver"
SALES_PATH = f"{company}/salesinvoiceline/ITEMSALE.csv"
OUTPUT_FILE = f"{company}/salesinvoiceheader/cleaned.csv"

EXCLUDE_HEADERS = [
    "Item Number", "Quantity", "Price",
    "Tax Amount", "Tax Code", "Freight Amount",
    "Freight Tax Amount", "Freight Tax Code"
]

def load_csvs(path):
    files = glob.glob(path)
    dfs = []
    for f in files:
        # Read CSV and USE FIRST ROW AS HEADER
        df = pd.read_csv(f, header=0, dtype=str, keep_default_na=False)
        dfs.append(df)

    return pd.concat(dfs, ignore_index=True)

def clean_sales(df):
    # Keep all columns except excluded
    NEW_HEADERS = [col for col in df.columns if col not in EXCLUDE_HEADERS]

    cleaned = df[NEW_HEADERS]

    # Drop duplicate invoice numbers
    cleaned = cleaned.drop_duplicates(subset=["Invoice No."], keep="first")

    return cleaned

# RUN
sl = load_csvs(SALES_PATH)

cleaned = clean_sales(sl)

# Remove blank rows
cleaned = cleaned.replace("", pd.NA).dropna(how="all")



cleaned.to_csv(OUTPUT_FILE, index=False)

print("Written:", OUTPUT_FILE)
