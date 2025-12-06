import pandas as pd
import glob

company = "willaid"
VEND_LEDGER_PATH = f"{company}/vendorledgerentry/filtered.csv"
VEND_PATH = f"{company}/vendor/SupplierMaster.csv"
OUTPUT_FILE = f"{company}/vendorledgerentry/cleaned_with_recordid.csv"

REAL_HEADERS = ["supplierinvnett","supplierinvtax","supplierinvtotal","ordernumber","receiptnumber","receiptdate","purchaseinvdate","supplier","purchaseinvstatus","purchaseinvreference", "vendorno"]


VEND_HEADERS = ["SUPPLIER", "vendorno"]



def load_csvs(path):
    files = glob.glob(path)
    dfs = []
    for f in files:
        df = pd.read_csv(f, dtype=str, keep_default_na=False)  # <-- remove header=None
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True)

def update_vendor_ledger(ledger_df, cust_df):
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
    vend_df = cust_df.fillna("").astype(str)


    # Build lookup for full name: "lastname,firstname"
    full_name_lookup = {}

    for _, row in vend_df.iterrows():
        last = row.get("SUPPLIER", "")
        recordid = row.get("vendorno", "").strip()

        full_name_key = norm(f"{last}").strip(",")

        if full_name_key:
            full_name_lookup[full_name_key] = recordid

    cleaned_rows = []

    print(full_name_lookup)

    for _, row in ledger_df.iterrows():
        row = row.tolist()

        try:
            (
                supplierinvnett,supplierinvtax,supplierinvtotal,ordernumber,receiptnumber,receiptdate,purchaseinvdate,supplier,purchaseinvstatus,purchaseinvreference
            ) = row
        except ValueError:
            continue

        # Normalize ledger customer_name (expected: lastname,firstname)
        
        ledger_key = norm(supplier)

        # Match on full normalized name
        recordid = full_name_lookup.get(ledger_key, "")
        print(recordid)

        cleaned_rows.append([
            supplierinvnett,supplierinvtax,supplierinvtotal,ordernumber,receiptnumber,receiptdate,purchaseinvdate,supplier,purchaseinvstatus,purchaseinvreference,
            recordid
        ])

    return pd.DataFrame(cleaned_rows, columns=REAL_HEADERS)





# RUN
sl = load_csvs(VEND_LEDGER_PATH)
cust = load_csvs(VEND_PATH)
cleaned = update_vendor_ledger(sl, cust)


cleaned.to_csv(OUTPUT_FILE, index=False)
print("Written:", OUTPUT_FILE)
