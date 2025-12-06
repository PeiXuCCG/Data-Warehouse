import pandas as pd

company = "willaid"
INPUT_FILE  = f"{company}/purchinvheader/PurchaseInvoices.csv"
OUTPUT_FILE = f"{company}/vendorledgerentry/filtered.csv"

# Columns we want to keep (all lowercase)
KEEP_COLS = [
    "supplierinvnett",
    "supplierinvtax",
    "supplierinvtotal",
    "ordernumber",
    "receiptnumber",
    "receiptdate",
    "purchaseinvdate",
    "supplier",
    "purchaseinvstatus",
    "purchaseinvreference"
]

def filter_columns(input_file, output_file):
    # Read file with all headers forced to lowercase
    df = pd.read_csv(input_file)
    df.columns = df.columns.str.lower()

    # Keep only existing columns from KEEP_COLS
    cols_to_keep = [c for c in KEEP_COLS if c in df.columns]

    missing = set(KEEP_COLS) - set(cols_to_keep)
    if missing:
        print("Warning: Missing columns:", missing)

    filtered = df[cols_to_keep]

    filtered.to_csv(output_file, index=False)
    print("Written:", output_file)


# Run
filter_columns(INPUT_FILE, OUTPUT_FILE)
