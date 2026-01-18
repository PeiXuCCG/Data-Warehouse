import pandas as pd
import glob

# -----------------------------
# Configuration
# -----------------------------
company = "ergo"

SALES_PATH = f"{company}/salesinvoiceline/sources/*.csv"
OUTPUT_FILE = f"{company}/salesinvoiceline/cleaned.csv"

HEADINGS = [
    "referencenumber","transactiondate","quantity","itemcode","description",
    "amount","taxcode","status","customerpono","billingaddress",
    "billingaddresscontact","notes","terms","shipvia","memo","freight",
    "jobno","comments","amountpaid","duedate","promiseddate","taxid",
    "salesperson","shiptoaddress","amttax","saletotal",
    "freighttaxamount","freighttaxcode","discountpct","costcentre",
    "volumediscountpct"
]

# -----------------------------
# Helpers
# -----------------------------
def is_date(value):
    """
    STRICT date detection. Only values with / or - are treated as dates.
    """
    if not value:
        return False

    value = str(value).strip()
    if "/" not in value and "-" not in value:
        return False

    try:
        pd.to_datetime(value, dayfirst=True)
        return True
    except Exception:
        return False


def is_customer_header_row(row):
    """
    Detects a customer header row.
    - Only column 0 has text
    - All other columns are empty
    - Excludes known headers like Total, Referencenumber, Customername
    """
    first = str(row.iloc[0]).strip()
    if not first:
        return False

    first_l = first.lower()
    if first_l.startswith(("total", "referencenumber", "customername")):
        return False

    # All other columns must be empty
    others = row.iloc[2:]
    for v in others:
        if str(v).strip():  # ignore empty strings
            return False

    return True


# -----------------------------
# Load CSVs safely
# -----------------------------
def load_csvs(path):
    dfs = []
    for f in glob.glob(path):
        df = pd.read_csv(
            f,
            dtype=str,
            keep_default_na=False,  # turn NaN into empty strings
            engine="python",        # required for multiline quoted fields
            sep=",",
            quotechar='"',
            skip_blank_lines=True
        )
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True)


# -----------------------------
# Main logic
# -----------------------------
sl = load_csvs(SALES_PATH)

rows = []
current_customer_name = None
current_customer_number = None

for _, row in sl.iterrows():

    first = str(row.iloc[0]).strip()
    second = str(row.iloc[1]).strip() if len(row) > 1 else ""

    # Skip blank rows
    if not first and not second:
        continue

    # Skip Total / repeated headers
    if first.lower().startswith(("total", "referencenumber", "customername")):
        current_customer_name = None
        current_customer_number = None
        continue

    # -------------------------
    # Customer header row
    # -------------------------
    if is_customer_header_row(row):
        print(first)
        current_customer_name = first
        current_customer_number = None  # set to None unless your CSV has number elsewhere
        continue

    # -------------------------
    # Transaction row
    # -------------------------
    if is_date(second):

        record = {
            "customernumber": current_customer_number,
            "customername": current_customer_name,
        }


        # Initialise all columns
        for h in HEADINGS:
            record[h] = ""

        # Assign by position
        for i, h in enumerate(HEADINGS):
            if i < len(row):
                record[h] = str(row.iloc[i]).strip()

        rows.append(record)



# -----------------------------
# Final DataFrame
# -----------------------------
final_cols = ["customernumber", "customername"] + HEADINGS
clean_df = pd.DataFrame(rows, columns=final_cols)

# -----------------------------
# Clean accounting-style numbers
# -----------------------------
for col in ["amount", "saletotal"]:
    if col in clean_df.columns:
        clean_df[col] = (
            clean_df[col]
            .astype(str)
            .str.replace(",", "", regex=False)
            .str.replace("$", "", regex=False)
            .str.replace("(", "-", regex=False)
            .str.replace(")", "", regex=False)
            .str.strip()
        )
        clean_df[col] = pd.to_numeric(
            clean_df[col],
            errors="coerce"
        ).fillna(0.0)

# -----------------------------
# Export
# -----------------------------
clean_df.to_csv(OUTPUT_FILE, index=False)

print(f"Cleaned Sales Detail exported to {OUTPUT_FILE} ({len(clean_df)} rows)")
