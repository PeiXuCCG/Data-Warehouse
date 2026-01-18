import pandas as pd
import glob

company = "ergo"
PATH = f"{company}/purchinvline/cleaned.csv"
OUTPUT_FILE = f"{company}/purchinvline/cleaned6.csv"

REAL_HEADERS = ["Record ID", "Purchase No.", "Item Number","Job", "Amount", "Shipping Date","Tax Code","Tax Amount","Freight Amount","Freight Tax Code","Freight Tax Amount"]
NUM_COLS = len(REAL_HEADERS)

def load_csvs(path):
    files = glob.glob(path)
    dfs = []
    for f in files:
        df = pd.read_csv(f, header=None, dtype=str, keep_default_na=False)
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True)

def clean_purchases(df):
    # Find header row by searching for REAL_HEADERS in the dataframe
    header_map = {}

    # Scan every column for each expected header value in the first row
    for col_index in range(df.shape[1]):
        value = str(df.iloc[0, col_index]).strip()
        if value in REAL_HEADERS:
            header_map[value] = col_index

    # Reorder columns in the order of REAL_HEADERS, include only those found
    selected_cols = []
    for h in REAL_HEADERS:
        if h in header_map:
            selected_cols.append(header_map[h])

    # Extract matched columns
    cleaned = df.iloc[:, selected_cols].copy()

    # Set proper column names
    cleaned.columns = [h for h in REAL_HEADERS if h in header_map]

    # Remove the row that contained the header labels
    cleaned = cleaned[1:].reset_index(drop=True)

    return cleaned
# RUN
sl = load_csvs(PATH)
cleaned = clean_purchases(sl)
cleaned.to_csv(OUTPUT_FILE, index=False)

print("Written:", OUTPUT_FILE)
