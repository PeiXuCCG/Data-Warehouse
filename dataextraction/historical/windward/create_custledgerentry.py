import pandas as pd
import os

company = "complexrehab"

# -----------------------------
# 1. LOAD CSV FILES
# -----------------------------
def clean_columns(df):
    if df.columns is not None and len(df.columns) > 0:
        df.columns = [str(c).strip().lower() for c in df.columns]
    return df

def to_numeric(df, col):
    if col in df.columns:
        df[col] = pd.to_numeric(
            df[col].astype(str).str.replace(",", "").str.replace("$", "", regex=False),
            errors="coerce"
        )
    else:
        # Column missing → create with zeros
        df[col] = 0
    return df

def load_csv_or_empty(path, parse_dates=None):
    """
    Load CSV file if it exists, otherwise return empty DataFrame.
    """
    if os.path.exists(path):
        return pd.read_csv(path, parse_dates=parse_dates)
    else:
        # Return empty DataFrame
        print(f"File not found: {path}. Returning empty DataFrame.")
        return pd.DataFrame()
# -----------------------------
# 1. LOAD AND CLEAN PAYMENTS CSV
# -----------------------------
payments_path = f"{company}/custledgerentry/payments.csv"
payments = load_csv_or_empty(payments_path, parse_dates=["Date"])
payments = clean_columns(payments)
payments = to_numeric(payments, "total") 

# -----------------------------
# 2. LOAD AND CLEAN SALES CSV
# -----------------------------
sales_path = f"{company}/salesinvline/sales.csv"
sales = load_csv_or_empty(sales_path, parse_dates=["Invoice Date"])
sales = clean_columns(sales)
sales = to_numeric(sales, "total")

# -----------------------------
# 2. RENAME COLUMNS FOR CONSISTENCY
# -----------------------------
payments = payments.rename(columns={
    "Customer": "customer",
    "Date": "date",
    "Total": "total"
})

sales = sales.rename(columns={
    "Customer": "customer",
    "Invoice Date": "date",
    "Total": "total"
})

print(sales.columns)
print(payments.columns)

# -----------------------------
# 3. CREATE LEDGER ENTRIES
# -----------------------------
# Invoices → debits
sales_ledger = sales.assign(
    entry_type="invoice",
    debit=sales["total"],
    credit=0
)

# Payments → credits
payments_ledger = payments.assign(
    entry_type="payment",
    debit=0,
    credit=payments["total"]
)

# -----------------------------
# 4. COMBINE LEDGERS
# -----------------------------
ledger = pd.concat([sales_ledger, payments_ledger], ignore_index=True)

print(ledger)
# Only keep valid customers
ledger = ledger[ledger["customer"].notnull()]

# Transaction amount
ledger["amount"] = ledger["debit"] - ledger["credit"]

# -----------------------------
# 5. SORT + RUNNING BALANCE PER CUSTOMER
# -----------------------------
ledger = ledger.sort_values(["customer", "date"])


# -----------------------------
# 6. FINAL LEDGER OUTPUT
# -----------------------------
customer_ledger = ledger[
    ["customer", "date", "entry_type", "debit", "credit"]
]

OUTPUT_FILE = f"{company}/custledgerentry/customer_ledger_entry.csv"
customer_ledger.to_csv(OUTPUT_FILE, index=False)
print(f"Customer Ledger Entry written to {OUTPUT_FILE}")