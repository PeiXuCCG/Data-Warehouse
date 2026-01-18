import pandas as pd
import glob

company = "thechairdoctor"
SALES_PATH = f"{company}/salesinvoiceline/cleaned4.csv"
OUTPUT_FILE = f"{company}/salesinvoiceline/cleaned_fix_splitlines.csv"


def load_csvs(path):
    files = glob.glob(path)
    dfs = []
    for f in files:
        df = pd.read_csv(f, header=0, dtype=str, keep_default_na=False)


        dfs.append(df)

    return pd.concat(dfs, ignore_index=True)

def clean_sales(sl: pd.DataFrame) -> pd.DataFrame:
    rows_to_drop = []

    for i in range(len(sl) - 1):
        row = sl.iloc[i]
        next_row = sl.iloc[i + 1]

        # Address-only row
        if (
            row.iloc[0] != ""
            and row.iloc[2] != ""
            and (row.iloc[3:] == "").all()
        ):
            name = row.iloc[0].strip()
            street = row.iloc[2].strip()
            suburb = next_row.iloc[0].strip()

            # Build full address
            full_address = f"{street} {suburb}".strip()

            # Convert next row to list so we can insert
            new_row = next_row.tolist()

            # Force name
            new_row[0] = name

            # Replace address column
            new_row[2] = full_address

            # INSERT two empty columns after address
            new_row.insert(3, "")
            new_row.insert(4, "")

            # Assign rebuilt row back
            sl.iloc[i + 1] = new_row[: len(sl.columns)]

            rows_to_drop.append(i)

    return sl.drop(index=rows_to_drop).reset_index(drop=True)





# RUN
sl = load_csvs(SALES_PATH)


cleaned = clean_sales(sl)
cleaned.to_csv(OUTPUT_FILE, index=False)

print("Written:", OUTPUT_FILE)
