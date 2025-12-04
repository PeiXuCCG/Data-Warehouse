import pandas as pd
import glob

company = "healthsaver"
PATH = f"{company}/purchinvline/ITEMPUR.csv"
OUTPUT_FILE = f"{company}/purchinvheader/cleaned.csv"

EXCLUDE_HEADERS = [
    "Item Number", "Quantity", "Price","Amount","Job",
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
    cleaned = cleaned.drop_duplicates(subset=["Purchase No."], keep="first")

    return cleaned

# RUN
sl = load_csvs(PATH)

cleaned = clean_sales(sl)

# Remove blank rows
cleaned = cleaned.replace("", pd.NA).dropna(how="all")

# Remove first row if needed
cleaned = cleaned.iloc[1:]

cleaned.to_csv(OUTPUT_FILE, index=False)

print("Written:", OUTPUT_FILE)
