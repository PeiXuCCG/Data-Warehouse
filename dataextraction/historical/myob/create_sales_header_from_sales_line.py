import pandas as pd

# -----------------------------
# Configuration
# -----------------------------
company = "healthsaver"
INPUT_FILE = f"{company}/salesinvoiceline/cleaned.csv"
OUTPUT_FILE = f"{company}/salesinvoiceheader/cleaned.csv"

# -----------------------------
# Load file
# -----------------------------
df = pd.read_csv(INPUT_FILE, dtype=str, keep_default_na=False)

# -----------------------------
# Clean amount column
# -----------------------------
df["amount"] = df["amount"].astype(float)

# -----------------------------
# Drop unnecessary columns
# -----------------------------
drop_cols = ["quantity", "itemcode"]
df = df.drop(columns=drop_cols, errors="ignore")

# -----------------------------
# Columns to keep aside from amount
# -----------------------------
keep_cols = [
    "customernumber",
    "customername",
    "referencenumber",
    "transactiondate",
    "taxcode",
    "status",
    "customerpono",
    "billingaddress",
    "billingaddresscontact",
    "notes",
    "terms",
    "shipvia",
    "memo",
    "freight",
    "jobno",
    "comments",
    "amountpaid",
    "duedate",
    "promiseddate",
    "taxid",
    "salesperson",
    "shiptoaddress",
    "saletotal",
    "freighttaxamount",
    "freighttaxcode",
    "discountpct",
    "costcentre",
    "volumediscountpct"
]

# Keep only columns that exist
keep_cols = [c for c in keep_cols if c in df.columns]

# -----------------------------
# Aggregate by referencenumber
# -----------------------------
# We'll take the first value for each column except amount, sum amount as total
agg_dict = {col: "first" for col in keep_cols if col != "amount"}
agg_dict["amount"] = "sum"

final_df = df.groupby("referencenumber", as_index=False).agg(agg_dict)

# Rename aggregated amount to total
final_df = final_df.rename(columns={"amount": "totalex"})

# -----------------------------
# Ensure totalex is numeric
# -----------------------------
final_df["totalex"] = pd.to_numeric(final_df["totalex"], errors="coerce").fillna(0.0)

# Round to 2 decimal places
final_df["totalex"] = final_df["totalex"].round(2)

# -----------------------------
# Export
# -----------------------------
final_df.to_csv(OUTPUT_FILE, index=False)

print(f"Invoice totals written to {OUTPUT_FILE} ({len(final_df)} rows)")
