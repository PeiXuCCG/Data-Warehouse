


import pandas as pd
import re

def clean(x):
    """Normalize cell text."""
    if pd.isna(x):
        return ""
    return re.sub(r"\s+", " ", str(x)).strip()

# ============================
# STEP 1: LOAD & CLEAN SHEET
# ============================

# --- Load the sheet ---
SHEET_NAME = "homecare_equipment/salesorder_source/xlsx/Quotes_by_Item-3.xlsx"
OUTPUT_PATH = "homecare_equipment/salesorderheader/output.csv"

df_raw = pd.read_excel(SHEET_NAME, header=None)
df = df_raw.map(clean)

records = []
current_block = {
    "Type": "",
    "Category": "",
    "Item": "",
    "Item Code": "",
    "Booked From": "",
    "Quote Number": "",
    "Customer": "",
    "Reference": "",
    "Total": "",
}

skip_next_line = False   # skip “line two” of every block

def finalize_block():
    if current_block["Quote Number"]:
        # Only save if we actually captured a detail row
        records.append(current_block.copy())

# ============================
# STEP 2: PARSE BLOCKS
# ============================

for idx, row in df.iterrows():
    # Ignore header-like rows containing "Quote Number"
    if any("Quote Number" in str(cell) for cell in row):
        continue

    col0 = row[0]

    # Skip the required line after Type/Category
    if skip_next_line:
        skip_next_line = False
        continue

    # Detect new Type/Category block starts
    if col0 not in ["", None]:
        # End previous block if any
        if current_block["Quote Number"]:
            finalize_block()
            current_block = {k: "" for k in current_block}

        # Set Type or Category
        if current_block["Type"] == "":
            current_block["Type"] = "" if col0 == "Type" else col0
        elif current_block["Category"] == "":
            current_block["Category"] = col0

        skip_next_line = True
        continue

    # Non-type rows:
    row_values = [v for v in row if v not in ["", None]]

    # Item description
    if current_block["Item"] == "" and len(row_values) == 1:
        current_block["Item"] = row_values[0]
        continue

    # Item Code
    if current_block["Item Code"] == "" and len(row_values) == 1:
        current_block["Item Code"] = row_values[0]
        continue

    # Detail row: date, quote number, customer, reference, qty, each, total
    if len(row_values) >= 4:
        # Some sheets may have variable spacing; take last value as total
        current_block["Booked From"]  = row_values[0]
        current_block["Quote Number"] = row_values[1]
        current_block["Customer"]     = row_values[2]
        current_block["Reference"]    = row_values[3]
        current_block["Total"]        = row_values[-1]  # last column is total
        continue

# Finalize last block
finalize_block()

# ============================
# STEP 3: CREATE PANDAS OUTPUT
# ============================

df_items = pd.DataFrame(records)

# Remove unwanted fields
df_items = df_items[[
    "Quote Number",
    "Booked From",
    "Customer",
    "Reference",
    "Total"
]]

# Convert Total to float
df_items["Total"] = (
    df_items["Total"]
    .astype(str)
    .str.replace(",", "")
    .astype(float)
)

# ============================
# STEP 4: GROUP BY QUOTE NUMBER
# ============================

df_quotes = (
    df_items
    .groupby("Quote Number", as_index=False)
    .agg({
        "Booked From": "first",
        "Customer": "first",
        "Reference": "first",
        "Total": "sum"
    })
)

# ============================
# STEP 5: SAVE CSV
# ============================

df_quotes.to_csv(OUTPUT_PATH, index=False)

print("Done! output.csv created.")
