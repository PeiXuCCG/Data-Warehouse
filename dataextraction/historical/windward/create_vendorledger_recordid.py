import pandas as pd
import glob

company = "tccg"
VENDOR_LEDGER_PATH = f"{company}/vendorledger/*.csv" 
SUPP_PATH = f"{company}/vendor/combined.csv" 
OUTPUT_FILE = f"{company}/vendorledger/cleaned_with_recordid.csv"


#REAL_HEADERS = ["Date","Src","ID No.","Memo","vendor_name","Transaction Amount","Balance","unknown_field","current_balance", "RecordID" ]
REAL_HEADERS = ["Date","Time","Supplier","BillCheque","Description","Amount","Balance", "RecordID"]


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
        last = row.get("Full Name", "")
        #print(last)
        recordid = row.get("recordid", "").strip()

        full_name_key = norm(f"{last}").strip(",")
        #print(full_name_key)


        if full_name_key:
            full_name_lookup[full_name_key] = recordid

    cleaned_rows = []

    print(ledger_df)


    for _, row in ledger_df.iterrows():
        row = row.tolist()

        try:
            (
               Date,Time,Supplier,BillCheque,Description,Amount,Balance 
            ) = row
        except ValueError:
            continue

        # Normalize ledger customer_name (expected: lastname,firstname)
        
        ledger_key = norm(Supplier)
        print(ledger_key)
        

        # Match on full normalized name
        recordid = full_name_lookup.get(ledger_key, "")
        #print(recordid)

        cleaned_rows.append([
           Date,Time,Supplier,BillCheque,Description,Amount,Balance,
            recordid
        ])



    return pd.DataFrame(cleaned_rows, columns=REAL_HEADERS)





# RUN
sl = load_csvs(VENDOR_LEDGER_PATH)
cust = load_csvs(SUPP_PATH)
cleaned = update_customer_ledger(sl, cust)


cleaned.to_csv(OUTPUT_FILE, index=False)
print("Written:", OUTPUT_FILE)
