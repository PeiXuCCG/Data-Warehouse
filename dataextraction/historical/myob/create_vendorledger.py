import pandas as pd
import glob

company = "healthsaver"
VENDOR_LEDGER_PATH = f"{company}/vendorledgerentry/vendorledgerentry.csv"
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

def clean_vendor_register(df):
    df = df.fillna("").astype(str)

    cleaned = []

    vendor_name = ""
    unknown_field = ""
    current_balance = ""

    for _, row in df.iterrows():
        row = [v.strip() for v in row.tolist()]

        # Skip totally empty rows
        if all(v == "" for v in row):
            continue

        # Skip TOTAL summary lines
        if any("total:" in v.lower() for v in row if v):
            continue

        # ===== DETECT VENDOR HEADER ROW (2 columns, not numeric) =====
        if len(row) >= 2 and row[0] and row[1] and not row[0].isdigit():
            vendor_name   = row[0]
            unknown_field = row[1]
            current_balance = ""     # reset until we see a balance
            continue

        # ===== DETECT BALANCE ROW (like ",,,,...,$33,662.42") =====
        if len(row) >= 6 and row[4] and row[5].replace("$","").replace(",","").replace("-","").isdigit():
            # we assume Amount field is a number = balance row
            current_balance = row[5]
            continue

        # ===== TRANSACTION ROW (first col is numeric) =====
        if row[0].isdigit():
            # pad
            row = row + [""] * (6 - len(row))
            child = row[:6]

            # attach vendor info
            child += [vendor_name, unknown_field, current_balance]

            cleaned.append(child)

    return pd.DataFrame(
        cleaned,
        columns=REAL_HEADERS + ["vendor_name", "unknown_field", "current_balance"]
    )



# RUN
sl = load_csvs(VENDOR_LEDGER_PATH)
cleaned = clean_vendor_register(sl)
cleaned.to_csv(OUTPUT_FILE, index=False)
print("Written:", OUTPUT_FILE)
