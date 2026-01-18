import pandas as pd
import glob

company = "ergo"
VENDOR_LEDGER_PATH = f"{company}/vendorledgerentry/supplierledger.csv"
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

def is_number(val):
    val = val.replace("$","").replace(",","").replace("-","").replace(".","")
    return val.isdigit()

def is_date(val):
    try:
        pd.to_datetime(val, errors="raise")
        return True
    except Exception:
        return False

def clean_vendor_register(df):
    df = df.fillna("").astype(str)
    cleaned = []

    vendor_name = ""
    unknown_field = ""
    current_balance = ""

    for _, row in df.iterrows():
        row = [v.strip() for v in row.tolist()]

        # Skip empty rows
        if all(v == "" for v in row):
            continue

        # Skip TOTAL rows
        if any("total:" in v.lower() for v in row if v):
            continue

        # Vendor header row (2 columns only)
        if row[0] and not is_date(row[0]):
            vendor_name = row[0]
            print("Vendor:", vendor_name)
            unknown_field = row[1] if len(row) > 1 else ""
            current_balance = ""
            continue

        # Balance row
        if not row[0] and not row[1] and not row[2] and is_number(row[5]):
            current_balance = row[5]
            continue

        # Transaction row
        if is_number(row[4]):
            row = row + [""] * (6 - len(row))
            child = row[:6]
            child += [vendor_name, unknown_field, current_balance]
            cleaned.append(child)

    return pd.DataFrame(
        cleaned,
        columns=REAL_HEADERS + EXTRA_HEADERS
    )




# RUN
sl = load_csvs(VENDOR_LEDGER_PATH)
cleaned = clean_vendor_register(sl)
cleaned.to_csv(OUTPUT_FILE, index=False)
print("Written:", OUTPUT_FILE)
