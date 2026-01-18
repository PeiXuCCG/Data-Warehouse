import pandas as pd
import glob
import re

company = "fisherlane"
GL_PATH = f"{company}/glentry/sources/*.csv"
OUTPUT_FILE = f"{company}/glentry/cleaned.csv"

HEADINGS = [
    "IDNo","Src","Date","Memo","Debit","Credit","JobNo.","NetActivity","EndingBalance"
]

def load_csvs(path):
    files = glob.glob(path)
    dfs = []
    for f in files:
        df = pd.read_csv(f, header=0, dtype=str, keep_default_na=False)
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True)

gl = load_csvs(GL_PATH)



rows = []

current_account_number = None
current_account_name = None

account_pattern = re.compile(r"^\d+-\d+")

for _, row in gl.iterrows():
    cells = [str(c).strip() for c in row if str(c).strip()]

    if not cells:
        continue

    first = cells[0]

    # Skip TOTAL rows
    if "total" in first.lower():
        current_account_number = None
        current_account_name = None
        continue

    # Detect account header anywhere in the row
    if account_pattern.match(first):
        current_account_number = first
        current_account_name = " ".join(cells[1:])
        continue

    # Detect transaction rows (date)
    try:
        pd.to_datetime(cells[2], dayfirst=True)
        is_transaction = True
    except:
        is_transaction = False

    if is_transaction and current_account_number:
        record = {
            "accountnumber": current_account_number,
            "accountname": current_account_name,
        }


        for i, h in enumerate(HEADINGS):
            record[h] = row[i] if i < len(row) else ""

        rows.append(record)

# ALWAYS create dataframe with columns
final_cols = ["accountnumber", "accountname"] + HEADINGS
clean_df = pd.DataFrame(rows, columns=final_cols)

# Export
clean_df.to_csv(OUTPUT_FILE, index=False)

print(f"Cleaned GL exported to {OUTPUT_FILE} ({len(clean_df)} rows)")
