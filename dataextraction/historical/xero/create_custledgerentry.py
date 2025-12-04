import pandas as pd
import glob
import csv

# ------------------------------------------
# CONFIG
# ------------------------------------------
company = "uccello_designs"
AR_ACCOUNTS = [
    "610", "611", "612", "620"
]
GL_PATH = f"{company}/glentry/*.csv"
RID_PATH = f"{company}/salesinvoiceline/*.csv"

# ------------------------------------------
# LOAD DATA
# ------------------------------------------


def load_csvs(path_glob):
    files = glob.glob(path_glob)
    if not files:
        raise ValueError(f"No CSV files found at {path_glob}")
    for f in files:
        print(f)
        pd.read_csv(f)


    return pd.concat([pd.read_csv(f) for f in files], ignore_index=True)

print("Loading GL Detail CSVs...")
gl = load_csvs(GL_PATH)
print(f"Loaded {len(gl)} GL rows.")

print("Loading Receivable Invoice Detail CSVs...")
rid = load_csvs(RID_PATH)
print(f"Loaded {len(rid)} RID rows.")

# ------------------------------------------
# CLEAN FIELD NAMES
# ------------------------------------------

def clean_cols(df):
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "")
        .str.replace("/", "")
    )
    return df

gl = clean_cols(gl)
rid = clean_cols(rid)

def fix_numeric(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = (
                df[c]
                .astype(str)
                .str.replace(",", "", regex=False)
                .str.replace(" ", "", regex=False)
                .str.replace("(", "-", regex=False)
                .str.replace(")", "", regex=False)
                .replace("", "0")
                .astype(float)
            )
    return df

# Clean GL numeric fields
gl = fix_numeric(gl, ["debit", "credit", "amount", "net", "gross", "taxamount"])

# Clean RID numeric fields
rid = fix_numeric(rid, ["quantity", "unitprice", "amount", "netamount", "grossamount"])

# ------------------------------------------
# FILTER GENERAL LEDGER TO AR CONTROL ACCOUNTS
# ------------------------------------------

gl['accountcode'] = gl['accountcode'].astype(str)
ar_gl = gl[gl['accountcode'].isin(AR_ACCOUNTS)].copy()
print(f"Filtered AR GL rows: {len(ar_gl)}")

# Ensure matching fields exist
ar_gl['reference'] = ar_gl.get('reference', '').astype(str)
ar_gl['journalid'] = ar_gl.get('journalid', '').astype(str)
rid['invoicenumber'] = rid.get('invoicenumber', '').astype(str)

# ------------------------------------------
# MATCH RULES
# ------------------------------------------
# To avoid column conflicts, prevent suffix chaos:
# We suffix GL columns with _gl manually, RID untouched.

ar_gl = ar_gl.add_suffix("_gl")
ar_gl = ar_gl.rename(columns={"reference_gl": "reference_gl", "journalid_gl": "journalid_gl"})

# ==========================================
# RULE 1 — Exact match: reference == invoicenumber
# ==========================================
step1 = ar_gl.merge(
    rid,
    left_on="reference_gl",
    right_on="invoicenumber",
    how="left"
)

# Unmatched rows after Rule 1
unmatched1 = step1[step1['invoicenumber'].isna()].copy()

# ==========================================
# RULE 2 — Invoice number appears inside description_gl
# ==========================================
unmatched1['description_gl'] = unmatched1['description_gl'].astype(str)

step2 = unmatched1.merge(
    rid,
    left_on="description_gl",
    right_on="invoicenumber",
    how="left",
    suffixes=("", "_r2")
)

# Remove duplicates from RID inside description rule
step2 = step2.drop_duplicates(subset=ar_gl.columns.tolist() + ["invoicenumber"])

# Combine step1 matched rows + step2 matched rows
matched_step1 = step1[~step1['invoicenumber'].isna()]
cle = pd.concat([matched_step1, step2], ignore_index=True)

# ==========================================
# RULE 3 — Match by JournalID
# ==========================================
unmatched2 = cle[cle['invoicenumber'].isna() & cle['journalid_gl'].notna()].copy()

journal_lookup = ar_gl[['journalid_gl', 'reference_gl']].drop_duplicates()

unmatched2 = unmatched2.merge(
    journal_lookup,
    on="journalid_gl",
    how="left",
    suffixes=("", "_jr")
)

# Fill missing invoice numbers using reference match
unmatched2['invoicenumber'] = unmatched2['invoicenumber'].fillna(unmatched2['reference_gl_jr'])

# Combine matched + rule3 results
matched_rule3 = unmatched2[~unmatched2['invoicenumber'].isna()]
cle = pd.concat([cle[~cle.index.isin(unmatched2.index)], matched_rule3], ignore_index=True)

# ------------------------------------------
# LABEL ENTRY TYPE
# ------------------------------------------

def classify_entry(row):
    debit = row.get('debit_gl', 0)
    credit = row.get('credit_gl', 0)
    if float(debit) > 0:
        return "invoice"
    elif float(credit) > 0:
        return "payment"
    else:
        return "adjustment"

cle['entry_type'] = cle.apply(classify_entry, axis=1)

# ------------------------------------------
# BUILD FINAL CUSTOMER LEDGER ENTRY
# ------------------------------------------

cle_final = pd.DataFrame({
    "date": cle['date_gl'],
    "contact": cle.get('contactname', cle.get('contact', None)),
    "invoice_number": cle['invoicenumber'],
    "entry_type": cle['entry_type'],
    "reference": cle['reference_gl'],
    "journal_id": cle['journalid_gl'],
    "debit": cle['debit_gl'],
    "credit": cle['credit_gl'],
    "gl_account": cle['accountcode_gl'],
    "due_date": cle.get('duedate', None),
    "description": cle['description_gl']
})

cle_final = cle_final.sort_values(["contact", "invoice_number", "date"])

# ------------------------------------------
# EXPORT
# ------------------------------------------

OUTPUT_FILE = f"{company}/custledgerentry/customer_ledger_entry.csv"
cle_final.to_csv(OUTPUT_FILE, index=False)
print(f"Customer Ledger Entry written to {OUTPUT_FILE}")
