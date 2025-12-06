import pandas as pd
import glob

company = "willaid"
CUST_LEDGER_PATH = f"{company}/custledgerentry/customer_ledger.csv"
CUST_PATH = f"{company}/customer/Customermaster.csv"
OUTPUT_FILE = f"{company}/custledgerentry/cleaned_with_recordid.csv"

REAL_HEADERS = ["Customer","DocumentNumber","DocumentType","DocumentDate","Debit","Credit","Notes","CustomerNo" ]

CUST_HEADERS = ["CUSTOMER", "customerno"]



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
        last = row.get("CUSTOMER", "")
        recordid = row.get("customerno", "").strip()

        full_name_key = norm(f"{last}").strip(",")

        if full_name_key:
            full_name_lookup[full_name_key] = recordid

    cleaned_rows = []

    print(full_name_lookup)

    for _, row in ledger_df.iterrows():
        row = row.tolist()

        try:
            (
                Customer,DocumentNumber,DocumentType,DocumentDate,Debit,Credit,Notes
            ) = row
        except ValueError:
            continue

        # Normalize ledger customer_name (expected: lastname,firstname)
        
        ledger_key = norm(Customer)

        # Match on full normalized name
        recordid = full_name_lookup.get(ledger_key, "")
        print(recordid)

        cleaned_rows.append([
            Customer,DocumentNumber,DocumentType,DocumentDate,Debit,Credit,Notes,
            recordid
        ])

    return pd.DataFrame(cleaned_rows, columns=REAL_HEADERS)





# RUN
sl = load_csvs(CUST_LEDGER_PATH)
cust = load_csvs(CUST_PATH)
cleaned = update_customer_ledger(sl, cust)


cleaned.to_csv(OUTPUT_FILE, index=False)
print("Written:", OUTPUT_FILE)
