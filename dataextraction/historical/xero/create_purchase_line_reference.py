import pandas as pd
import glob
import numpy as np

# ------------------------------------------
# CONFIG
# ------------------------------------------
company = "uccello_marketing_eu"
PURCHASE_PATH = f"{company}/purchinvline/jul25-dec25.csv"
PURCHASE_HEADER_PATH = f"{company}/purchinvheader/header_withreference_2.csv"


# ------------------------------------------
# LOAD CSV FILES
# ------------------------------------------
def load_csvs(path_glob):
    files = glob.glob(path_glob)
    if not files:
        raise ValueError(f"No CSV files found at {path_glob}")

    for f in files:
        print(f"Loading {f}")

    return pd.concat(
        [pd.read_csv(f, dtype=str) for f in files],
        ignore_index=True
    )


# ------------------------------------------
# CLEAN COLUMN NAMES ONLY
# ------------------------------------------
def clean_cols(df):
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "")
        .str.replace("/", "")
        .str.replace("%", "")
    )
    return df


print("Loading Purchase Invoice Line CSVs...")
pl = load_csvs(PURCHASE_PATH)

print("Loading Purchase Invoice Header CSVs...")
ph = load_csvs(PURCHASE_HEADER_PATH)

pl = clean_cols(pl)
ph = clean_cols(ph)


# ------------------------------------------
# NORMALIZE MERGE KEYS (CASE PRESERVED)
# ------------------------------------------
for df in (pl, ph):
    df["contact"] = df["contact"].astype(str).str.strip()
    df["invoicedate"] = pd.to_datetime(
        df["invoicedate"], errors="coerce"
    ).dt.date


# ------------------------------------------
# ENSURE REQUIRED COLUMNS EXIST
# ------------------------------------------
if "ordernumber" not in pl.columns:
    pl["ordernumber"] = np.nan

if "reference" not in pl.columns:
    pl["reference"] = np.nan


# ------------------------------------------
# DEDUPLICATE HEADER TO PREVENT MANY-TO-MANY MERGE
# ------------------------------------------
ph_unique = ph.drop_duplicates(subset=["contact", "invoicedate"])


# ------------------------------------------
# MERGE HEADER INTO LINES
# ------------------------------------------
merge_cols = ["contact", "invoicedate"]

pl = pl.merge(
    ph_unique[merge_cols + ["reference", "ordernumber"]],
    on=merge_cols,
    how="left",           # keep all line rows
    suffixes=("", "_ph")
)


# ------------------------------------------
# BACKFILL FROM HEADER
# ------------------------------------------
pl["reference"] = pl["reference"].fillna(pl["reference_ph"])
pl["ordernumber"] = pl["ordernumber"].fillna(pl["ordernumber_ph"])


# ------------------------------------------
# CLEANUP
# ------------------------------------------
pl = pl.drop(columns=["reference_ph", "ordernumber_ph"])
pl["ordernumber"] = pl["ordernumber"].astype("string")
pl = pl.drop_duplicates()


# ------------------------------------------
# OUTPUT
# ------------------------------------------
OUTPUT_FILE = f"{company}/purchinvline/lines_withreference_2.csv"
pl.to_csv(OUTPUT_FILE, index=False)

print(f"File written to: {OUTPUT_FILE}")
