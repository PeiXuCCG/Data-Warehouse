import pandas as pd
import glob

company = "countrycaremelbourne"
VENDOR_LEDGER_PATH = f"{company}/purchinvheader/cleaned.csv" 
SUPP_PATH = f"{company}/vendor/combined.csv" 
OUTPUT_FILE = f"{company}/purchinvheader/cleaned_with_recordid.csv"


#REAL_HEADERS = ["Date","Src","ID No.","Memo","vendor_name","Transaction Amount","Balance","unknown_field","current_balance", "RecordID" ]
REAL_HEADERS = ["PO #","Description","Date Received","Date Expected","PO Comment","Supplier","Department","Total", "SupplierRecordID"]


def load_csvs(path):
    files = glob.glob(path)
    dfs = []
    for f in files:
        df = pd.read_csv(f, dtype=str, keep_default_na=False, encoding="utf-8-sig")  # <-- remove header=None
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

    print(cust_df.columns)

    for _, row in cust_df.iterrows():
        last = row.get("Name", "")
        recordid = row.get("recordid", "").strip()


        full_name_key = norm(f"{last}").strip(",")


        if full_name_key:
            full_name_lookup[full_name_key] = recordid

    cleaned_rows = []



    for _, row in ledger_df.iterrows():
        row = row.tolist()

        try:
            (
               PO,Description,DateReceived,DateExpected,POComment,Supplier,Department,Total
            ) = row
        except ValueError:
            continue

        # Normalize ledger customer_name (expected: lastname,firstname)
        
        ledger_key = norm(Supplier)



        # Match on full normalized name
        recordid = full_name_lookup.get(ledger_key, "")

        cleaned_rows.append([
          PO,Description,DateReceived,DateExpected,POComment,Supplier,Department,Total,
            recordid
        ])



    return pd.DataFrame(cleaned_rows, columns=REAL_HEADERS)





# RUN
sl = load_csvs(VENDOR_LEDGER_PATH)
cust = load_csvs(SUPP_PATH)
cleaned = update_customer_ledger(sl, cust)


cleaned.to_csv(OUTPUT_FILE, index=False)
print("Written:", OUTPUT_FILE)


