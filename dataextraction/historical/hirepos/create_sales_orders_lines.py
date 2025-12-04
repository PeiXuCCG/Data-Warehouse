


import pandas as pd
import re

def clean(x):
    if pd.isna(x): 
        return ""
    return re.sub(r"\s+", " ", str(x)).strip()

# --- Load the sheet ---
SHEET_NAME = "homecare_equipment/salesorder_source/xlsx/Quotes_by_Item-3.xlsx"
OUTPUT_PATH = "homecare_equipment/salesorderline/output.csv"
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
    "Qty": "",
    "Each": "",
    "Total": ""
}

def finalize_block():
    if current_block["Item"]:
        records.append(current_block.copy())

skip_next_line = False   # skip the "line 2" rule

for idx, row in df.iterrows():
    # Convert entire row to a single string for checking text
    row_text = " ".join([str(x) for x in row if x not in ["", None]])

    # --- NEW RULE: Skip rows containing "Quote Number" ---
    if "quote number" in row_text.lower():
        continue

    # If flagged, skip this line entirely
    if skip_next_line:
        skip_next_line = False
        continue

    col0 = row[0]

    # --- New block begins when column 0 has text ---
    if col0 not in ["", None]:
        # If previous block is complete, save it
        if current_block["Item"]:
            finalize_block()
            current_block = {k: "" for k in current_block}


        # Fill Type then Category
        if current_block["Type"] == "":
            if col0.strip().lower() != "type":
                current_block["Type"] = col0
        elif current_block["Category"] == "":
            if col0.strip().lower() != "category":
                current_block["Category"] = col0

        # Skip the next line after this one
        skip_next_line = True
        continue

    # --- Inside a block ---
    row_values = [x for x in row if x not in ["", None]]

    # item description
    if current_block["Item"] == "" and len(row_values) == 1:
        current_block["Item"] = row_values[0]
        continue

    # item code
    if current_block["Item Code"] == "" and len(row_values) == 1:
        current_block["Item Code"] = row_values[0]
        continue

    # details row
    if len(row_values) >= 7:
        current_block["Booked From"]  = row_values[0]
        current_block["Quote Number"] = row_values[1]
        current_block["Customer"]     = row_values[2]
        current_block["Reference"]    = row_values[3]
        current_block["Qty"]          = row_values[4]
        current_block["Each"]         = row_values[5]
        current_block["Total"]        = row_values[6]

# finalize last record
finalize_block()

# --- Create final dataframe and export ---
df_final = pd.DataFrame(records)
df_final.to_csv(OUTPUT_PATH, index=False)
