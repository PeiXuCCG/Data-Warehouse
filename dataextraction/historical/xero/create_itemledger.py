import pandas as pd
import glob
import numpy as np

company = "open_mobility"
PURCHASES_PATH = f"{company}/purchinvline/*.csv"
SALES_PATH = f"{company}/salesinvoiceline/*.csv"


# ----------------------------
# LOAD + CLEAN
# ----------------------------
def load_csvs(path_glob):
    files = glob.glob(path_glob)
    if not files:
        raise ValueError(f"No CSV files found at {path_glob}")
    dfs = []
    for f in files:
        print("Loading:", f)
        df = pd.read_csv(f)
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True)


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


# Load and clean
sl = clean_cols(load_csvs(SALES_PATH))
rid = clean_cols(load_csvs(PURCHASES_PATH))

print(f"Loaded {len(sl)} sales invoice line rows")
print(f"Loaded {len(rid)} purchase invoice line rows")


# ----------------------------
# BUILD ITEM LEDGER
# ----------------------------

# Sales quantities become negative

sl["quantity"] = pd.to_numeric(sl["quantity"], errors="coerce")

# Sales cost unknown initially
sl["unitcost"] = np.nan

# Combine purchases + sales
ledger = pd.concat([rid, sl], ignore_index=True)

# Convert date to datetime for sorting
ledger["invoicedate"] = pd.to_datetime(ledger["invoicedate"])

# Sort to calculate running values correctly
ledger = ledger.sort_values(["itemcode", "invoicedate"])


# ----------------------------
# MOVING AVERAGE COST LOGIC
# ----------------------------

ledger["purchasevalue"] = np.where(
    pd.to_numeric(ledger["quantity"], errors="coerce") > 0,
    ledger["quantity"] * ledger["unitcost"],
    0
)

# Create columns
ledger["avgcost"] = np.nan
ledger["unitcost_final"] = np.nan
ledger["extcost"] = np.nan
ledger["runningqty"] = np.nan
ledger["runningvalue"] = np.nan


# Process item-by-item
for item, group in ledger.groupby("itemcode", sort=False):
    
    cum_qty = 0
    cum_value = 0
    
    for idx, row in group.iterrows():
        qty = row["quantity"]
        uc = row["unitcost"]
        
        if qty > 0:
            # purchase
            purchase_value = qty * uc
            cum_qty += qty
            cum_value += purchase_value
            avg_cost = cum_value / cum_qty if cum_qty != 0 else 0
            ledger.at[idx, "avgcost"] = avg_cost
            ledger.at[idx, "unitcost_final"] = uc
        
        else:
            # sale
            avg_cost = cum_value / cum_qty if cum_qty != 0 else 0
            ledger.at[idx, "avgcost"] = avg_cost
            ledger.at[idx, "unitcost_final"] = avg_cost
            cum_qty += qty
            cum_value += qty * avg_cost  # negative
            
        ledger.at[idx, "extcost"] = qty * ledger.at[idx, "unitcost_final"]
        ledger.at[idx, "runningqty"] = cum_qty
        ledger.at[idx, "runningvalue"] = cum_value


# ----------------------------
# WRITE TO CSV
# ----------------------------

output_file = f"{company}/itemledgerentry/item_ledger.csv"
ledger.to_csv(output_file, index=False)

print("Item ledger saved to:", output_file)
