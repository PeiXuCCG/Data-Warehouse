import pandas as pd
import glob

company = "the chair doctor"
SALES_PATH = f"{company}/valueledger/*.csv"
OUTPUT_FILE = f"{company}/valueledger/cleaned.csv"

REAL_HEADERS = ["IDNo","Src","Date","Memo","Debit","Credit"]
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

        # --- Detect ITEM HEADER rows (ItemCode + ItemName only) ---
              # --- Detect ITEM HEADER rows (ItemCode + ItemName only) ---
        if colA and colB and all(c == "" for c in row[2:]):
            current_item_code = colA
            current_item_name = colB
            continue  # do not include header row in output

        # --- Skip rows where IDNo + Src are blank (the second line in your example) ---
        if colA == "" and colB == "":
            continue

        # --- Normal data row ---
        if current_item_code:
            cleaned.append(row[:NUM_COLS] + [current_item_code, current_item_name])


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
