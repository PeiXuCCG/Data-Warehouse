import pandas as pd
import glob

company = "ergo"
SALES_PATH = f"{company}/itemledgerentry/*.csv"
OUTPUT_FILE = f"{company}/itemledgerentry/cleaned.csv"

REAL_HEADERS = ["Date","Src","IDNo","Memo","Starting Qty","Qty Changed","Amount","On Hand","Current Value"]
NUM_COLS = len(REAL_HEADERS)

def load_csvs(path):
    files = glob.glob(path)
    dfs = []
    for f in files:
        df = pd.read_csv(f, header=None, dtype=str, keep_default_na=False)
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True)

def clean_item_register(df):
    # 1. Drop unnamed / empty trailing columns
    df = df.loc[:, :NUM_COLS-1].copy()

    df = df.fillna("").astype(str)

    cleaned = []
    current_item_code = None
    current_item_name = None

    for _, row in df.iterrows():
        row = [c.strip() for c in row]

        colA, colB = row[0], row[1]

        # --- Skip full blank rows ---
        if all(c == "" for c in row):
            continue

        # --- Detect TOTAL rows ---
        if any("total" in c.lower() for c in row[:NUM_COLS] if c):
            current_item_code = None
            current_item_name = None
            continue   # remove total row

        # --- Detect ITEM HEADER rows (ItemCode + ItemName only) ---
        if colA and colB and all(c == "" for c in row[2:]):
            current_item_code = colA
            current_item_name = colB
            continue  # do not include header row in output

        # --- Normal data row ---
        # Must have a Name but NOT be a header row
        if current_item_code:
            # Append item info
            cleaned.append(row[:NUM_COLS] + [current_item_code, current_item_name])

    result = pd.DataFrame(cleaned, columns=REAL_HEADERS + ["itemcode", "itemname"])
    return result

# RUN
sl = load_csvs(SALES_PATH)
cleaned = clean_item_register(sl)
cleaned.to_csv(OUTPUT_FILE, index=False)

print("Written:", OUTPUT_FILE)
