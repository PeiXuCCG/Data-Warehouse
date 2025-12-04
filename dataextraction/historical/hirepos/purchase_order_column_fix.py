import pandas as pd
import glob

company = "lakeside_mobility"
CUSTOMER_PATH = f"{company}/*.csv"
OUTPUT_FILE = f"{company}/output.csv"

headers = ['OrderDate', 'OrderNo', 'Status', 'DateReceived', 'Total']


def load_csvs(path_glob):
    files = glob.glob(path_glob)
    if not files:
        raise ValueError(f"No CSV files found at {path_glob}")

    print("Files loaded:")
    for f in files:
        print(f)

    return pd.concat([pd.read_csv(f) for f in files], ignore_index=True)


def clean_cols(df):
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "", regex=False)
        .str.replace("_", "", regex=False)
        .str.replace("/", "", regex=False)
        .str.replace("%", "", regex=False)
    )
    return df


# -------------------------------------------------
# LOAD + CLEAN
# -------------------------------------------------
sl = load_csvs(CUSTOMER_PATH)
sl = clean_cols(sl)

# sl columns after clean():
# orderdate, itemtype, itemcategory, itemdescription,
# quantity, orderdate, orderno, status, supplier,
# datereceived, amount

# Ensure 'amount' is numeric
sl["amount"] = pd.to_numeric(sl["amount"], errors="coerce")


# -------------------------------------------------
# GROUP BY ORDERNO AND SUM AMOUNT
# -------------------------------------------------
grouped = (
    sl.groupby("orderno")
    .agg({
        "orderdate": "first",
        "status": "first",
        "datereceived": "first",
        "amount": "sum"
    })
    .reset_index()
)

grouped.rename(columns={
    "orderno": "OrderNo",
    "orderdate": "OrderDate",
    "status": "Status",
    "datereceived": "DateReceived",
    "amount": "Total"
}, inplace=True)

# Reorder columns
grouped = grouped[headers]


# -------------------------------------------------
# WRITE OUT FILE
# -------------------------------------------------
grouped.to_csv(OUTPUT_FILE, index=False)
print(f"file written to {OUTPUT_FILE}")
