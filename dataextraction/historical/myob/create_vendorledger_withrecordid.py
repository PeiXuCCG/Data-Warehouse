import pandas as pd
import glob

company = "ergo"
VENDOR_LEDGER_PATH = f"{company}/vendorledgerentry/vendorledgerentry.csv" 
SUPP_PATH = f"{company}/vendor/SUPPLIERS.csv" 
OUTPUT_FILE = f"{company}/vendorledgerentry/cleaned_with_recordid.csv"


#REAL_HEADERS = ["Date","Src","ID No.","Memo","vendor_name","Transaction Amount","Balance","unknown_field","current_balance", "RecordID" ]
REAL_HEADERS = ["Date","PONo","SupplierInvNo","Supplier Name","Amount","Amount Due","Status","Received", "RecordID"]


def load_csvs(path):
    files = glob.glob(path)
    dfs = []
    for f in files:
        df = pd.read_csv(f, dtype=str, keep_default_na=False)  # <-- remove header=None
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True)

def update_customer_ledger(ledger_df, cust_df):
    # Normalize: lowercase, strip, remove spaces, remove quotes
    def norm(s):
        return (
            s.strip()
             .replace(" ", "")
             .replace('"', "")
             .replace("'", "")
             .lower()
        )

    ledger_df = ledger_df.fillna("").astype(str)
    cust_df = cust_df.fillna("").astype(str)



    # Build lookup for full name: "lastname,firstname"
    full_name_lookup = {}

    for _, row in cust_df.iterrows():
        last = row.get("CoLastName", "")
        recordid = row.get("RecordID", "").strip()

        full_name_key = norm(f"{last}").strip(",")

        if full_name_key:
            full_name_lookup[full_name_key] = recordid

    cleaned_rows = []




    for _, row in ledger_df.iterrows():
        row = row.tolist()

        try:
            (
                Date,PONo,SupplierInvNo,SupplierName,Amount,AmountDue,Status,Received
            ) = row
        except ValueError:
            continue

        # Normalize ledger customer_name (expected: lastname,firstname)
        
        ledger_key = norm(SupplierName)

        # Match on full normalized name
        recordid = full_name_lookup.get(ledger_key, "")

        cleaned_rows.append([
            Date,PONo,SupplierInvNo,SupplierName,Amount,AmountDue,Status,Received,
            recordid
        ])

    return pd.DataFrame(cleaned_rows, columns=REAL_HEADERS)





# RUN
sl = load_csvs(VENDOR_LEDGER_PATH)
cust = load_csvs(SUPP_PATH)
cleaned = update_customer_ledger(sl, cust)


cleaned.to_csv(OUTPUT_FILE, index=False)
print("Written:", OUTPUT_FILE)
