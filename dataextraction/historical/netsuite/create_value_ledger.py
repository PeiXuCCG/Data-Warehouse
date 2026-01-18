import pandas as pd
import re
import glob

company = "vital_living"

# -----------------------------
# 1. HELPERS
# -----------------------------

def clean_columns(df):
    cleaned_cols = []
    for c in df.columns:
        col = str(c).strip().lower()
        col = re.sub(r'\s+', '', col)
        col = re.sub(r'[^a-z0-9_]', '', col)
        col = re.sub(r'_+', '', col)
        cleaned_cols.append(col)
    df.columns = cleaned_cols
    return df


def load_csv_or_empty_glob(path_pattern):
    files = glob.glob(path_pattern)
    if not files:
        print(f"No files found for pattern: {path_pattern}")
        return pd.DataFrame()

    return pd.concat(
        (pd.read_csv(f, encoding="latin1") for f in files),
        ignore_index=True
    )


# -----------------------------
# 2. LOAD VALUE LEDGER
# -----------------------------

inventory_costs_path = f"{company}/valueledgerentry/sources/*.csv"

value_ledger = load_csv_or_empty_glob(inventory_costs_path)
value_ledger = clean_columns(value_ledger)

if value_ledger.empty:
    print("No value ledger data found.")
    exit(0)

# -----------------------------
# 3. PREP ITEM COLUMN FOR FILL
# -----------------------------

# Normalise item column
value_ledger["item"] = (
    value_ledger["item"]
    .astype(str)
    .str.strip()
    .replace({"": pd.NA, "nan": pd.NA})
)

# Clear section headers and totals so they DON'T get filled
value_ledger.loc[
    value_ledger["item"].str.lower().isin(
        ["assembly/billofmaterials", "inventoryitem", "widget"]
    ),
    "item"
] = pd.NA

value_ledger.loc[
    value_ledger["item"].str.lower().str.startswith("total -", na=False),
    "item"
] = pd.NA

# 🔑 Forward-fill item DOWN the file
value_ledger["item"] = value_ledger["item"].ffill()

# -----------------------------
# 4. FILTER TO TRANSACTIONS
# -----------------------------

# Normalise transaction type
value_ledger["transactiontype"] = (
    value_ledger["transactiontype"]
    .astype(str)
    .str.strip()
    .str.lower()
)

detail_df = value_ledger[
    value_ledger["transactiontype"].isin(
        ["item receipt", "item fulfillment"]
    )
].copy()

# Safety: drop rows that still somehow lack item
detail_df = detail_df[detail_df["item"].notna()]

# -----------------------------
# 5. WRITE OUTPUT
# -----------------------------

OUTPUT_FILE = f"{company}/valueledgerentry/cleaned.csv"
detail_df.to_csv(OUTPUT_FILE, index=False)

print(f"Value Ledger Entry written to {OUTPUT_FILE}")
