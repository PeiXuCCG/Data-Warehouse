import pandas as pd
import os

company = "vital_living"

# -----------------------------
# 1. LOAD CSV FILES
# -----------------------------
import re

def clean_columns(df):
    if df.columns is not None and len(df.columns) > 0:
        cleaned_cols = []
        for c in df.columns:
            col = str(c).strip().lower()            # lowercase + trim
            col = re.sub(r'\s+', '', col)          # replace spaces with underscore
            col = re.sub(r'[^a-z0-9_]', '', col)    # remove special characters
            col = re.sub(r'_+', '', col)           # collapse multiple underscores                   # remove leading/trailing underscores
            cleaned_cols.append(col)
        df.columns = cleaned_cols
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
        return pd.read_csv(path, parse_dates=parse_dates, encoding="latin1")
    else:
        # Return empty DataFrame
        print(f"File not found: {path}. Returning empty DataFrame.")
        return pd.DataFrame()
# -----------------------------
# 1. LOAD AND CLEAN PAYMENTS CSV
# -----------------------------
payments_path = f"{company}/vendorledgerentry/payment.csv"
payments = load_csv_or_empty(payments_path, parse_dates=["Date Created"])
payments = clean_columns(payments)
payments = to_numeric(payments, "amount") 

# -----------------------------
# 2. LOAD AND CLEAN SALES CSV
# -----------------------------
purch_path = f"{company}/vendorledgerentry/Header.csv"
purch = load_csv_or_empty(purch_path, parse_dates=["Date Created"])
purch = clean_columns(purch)
purch = to_numeric(purch, "amount")

# -----------------------------
# 2. RENAME COLUMNS FOR CONSISTENCY
# -----------------------------
payments = payments.rename(columns={
    "internalid": "vendorno"
})

purch = purch.rename(columns={
    "internalid": "vendorno"
})

print(purch.columns)
print(payments.columns)

# -----------------------------
# 3. CREATE LEDGER ENTRIES
# -----------------------------
# Invoices → debits
purch_ledger = purch.assign(
    entry_type="invoice",
    debit=purch["amount"],
    credit=0
)

# Payments → credits
payments_ledger = payments.assign(
    entry_type="payment",
    debit=0,
    credit=payments["amount"]
)

# -----------------------------
# 4. COMBINE LEDGERS
# -----------------------------
ledger = pd.concat([purch_ledger, payments_ledger], ignore_index=True)

print(ledger)
# Only keep valid customers
ledger = ledger[ledger["vendorno"].notnull()]

# Transaction amount
ledger["amount"] = ledger["debit"] - ledger["credit"]

# -----------------------------
# 5. SORT + RUNNING BALANCE PER CUSTOMER
# -----------------------------
ledger = ledger.sort_values(["vendorno", "date"])


# -----------------------------
# 6. FINAL LEDGER OUTPUT
# -----------------------------
vendor_ledger = ledger[
    ["vendorno", "date", "entry_type", "debit", "credit"]
]

OUTPUT_FILE = f"{company}/vendorledgerentry/cleaned.csv"
vendor_ledger.to_csv(OUTPUT_FILE, index=False)
print(f"Vendor Ledger Entry written to {OUTPUT_FILE}")