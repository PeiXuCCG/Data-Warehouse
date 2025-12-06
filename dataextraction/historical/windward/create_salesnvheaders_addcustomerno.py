import pandas as pd
import glob

company = "tccg"
SALES_INV_HEADER_PATH = f"{company}/salesinvheader/cleaned.csv" 
CUST_PATH = f"{company}/customer/customer.csv" 
OUTPUT_FILE = f"{company}/salesinvheader/cleaned_with_recordid.csv"


#REAL_HEADERS = ["Date","Src","ID No.","Memo","vendor_name","Transaction Amount","Balance","unknown_field","current_balance", "RecordID" ]
REAL_HEADERS = [
    "InvoiceNumber",
    "Number",
    "Customer",
    "CustomerAcct",
    "PO",
    "SubTotal",
    "Taxes",
    "Total",
    "Cost",
    "Profit",
    "Margin",
    "InvoiceDate",
    "OrderedDate",
    "CreationDate",
    "DateofE",
    "DateofW",
    "DateofA",
    "InvoiceRefNo",
    "CustomerPart",
    "ShipTo",
    "Delivery",
    "DeliveryPhone",
    "DeliveryEmail",
    "DeliveryNotes",
    "DaystoPay",
    "BalanceDue",
    "Salesperson",
    "Technicians",
    "RentalState",
    "RentalOut",
    "RentalIn",
    "Comments",
    "CustomerNo"
]



def load_csvs(path):
    files = glob.glob(path)
    dfs = []
    for f in files:
        df = pd.read_csv(f, dtype=str, keep_default_na=False, encoding='utf-8-sig')  # <-- remove header=None
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
        recordid = row.get("RecordId", "").strip()

        full_name_key = norm(f"{last}").strip(",")


        if full_name_key:
            full_name_lookup[full_name_key] = recordid

    cleaned_rows = []





    for _, row in ledger_df.iterrows():
        row = row.tolist()

        try:
            (
               InvoiceNumber,Number,Customer,CustomerAcct,PO,SubTotal,Taxes,Total,Cost,Profit,Margin,InvoiceDate,OrderedDate,CreationDate,DateofE,DateofW,DateofA,InvoiceRefNo,CustomerPart,ShipTo,Delivery,DeliveryPhone,DeliveryEmail,DeliveryNotes,DaystoPay,BalanceDue,Salesperson,Technicians,RentalState,RentalOut,RentalIn,Comments
            ) = row
        except ValueError:
            raise
            #continue

        # Normalize ledger customer_name (expected: lastname,firstname)
        
        ledger_key = norm(Customer)

        # Match on full normalized name
        recordid = full_name_lookup.get(ledger_key, "")

        cleaned_rows.append([
           InvoiceNumber,Number,Customer,CustomerAcct,PO,SubTotal,Taxes,Total,Cost,Profit,Margin,InvoiceDate,OrderedDate,CreationDate,DateofE,DateofW,DateofA,InvoiceRefNo,CustomerPart,ShipTo,Delivery,DeliveryPhone,DeliveryEmail,DeliveryNotes,DaystoPay,BalanceDue,Salesperson,Technicians,RentalState,RentalOut,RentalIn,Comments,
           recordid
        ])



    return pd.DataFrame(cleaned_rows, columns=REAL_HEADERS)





# RUN
sl = load_csvs(SALES_INV_HEADER_PATH)
cust = load_csvs(CUST_PATH)
cleaned = update_customer_ledger(sl, cust)


cleaned.to_csv(OUTPUT_FILE, index=False)
print("Written:", OUTPUT_FILE)


