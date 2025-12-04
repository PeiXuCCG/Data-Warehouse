import pandas as pd
import glob

company = "fisherlane"
SALES_PATH = f"{company}/vendorledgerentry/vendorledger.csv"
OUTPUT_FILE = f"{company}/vendorledgerentry/cleaned.csv"

REAL_HEADERS = ["Date","Src","ID No.","Memo","Transaction Amount","Balance"]

# Additional fields to attach
EXTRA_HEADERS = ["vendor_name", "unknown_field", "current_balance"]

def load_csvs(path):
    files = glob.glob(path)
    dfs = []
    for f in files:
        df = pd.read_csv(f, dtype=str, keep_default_na=False, header=None)
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True)

def clean_item_register(df):
    df = df.fillna("").astype(str)

    cleaned = []

    # Track the current parent header values
    customer_name = ""
    unknown_field = ""
    current_balance = ""

    for _, row in df.iterrows():
        row = [v.strip() for v in row.tolist()]

        # Remove TOTAL rows
        if any("total" in v.lower() for v in row if v):
            continue

        # ===== HEADER ROW (3 columns) =====
        if len(row) >= 3 and row[0] and row[1] and row[2] and all(v == "" for v in row[3:]):
            customer_name   = row[0]
            unknown_field   = row[1]
            current_balance = row[2]
            continue  # do NOT output this row

        # ===== CHILD TRANSACTION ROW (normal data) =====
        # Ensure it has at least 6 columns (pad if needed)
        row = row + [""] * (6 - len(row))
        child = row[:6] + [customer_name, unknown_field, current_balance]
        cleaned.append(child)

    # Build dataframe
    return pd.DataFrame(cleaned, columns=REAL_HEADERS + EXTRA_HEADERS)


# RUN
sl = load_csvs(SALES_PATH)
cleaned = clean_item_register(sl)
cleaned.to_csv(OUTPUT_FILE, index=False)
print("Written:", OUTPUT_FILE)
