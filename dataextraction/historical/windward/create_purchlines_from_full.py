import pandas as pd

company = "countrycaremelbourne"
input_file = f"{company}/purchline/purchase_30Jun.csv"
output_file = f"{company}/purchline/cleaned_lines_flat_2.csv"

# Load CSV
df = pd.read_csv(input_file, dtype=str, encoding='latin1').fillna("")


# Columns to forward-fill from header rows
header_cols = [
    "Number",
    "Date",
    "Supplier",
    "Ordered",
    "On Order",
    "Received"
]

# Identify line-item rows (rows where Item or Description exists)
is_line = (df["Item"].notna() & df["Item"].str.strip().ne("")) | \
          (df["Description"].notna() & df["Description"].str.strip().ne(""))

# Forward-fill header information
df[header_cols] = df[header_cols].replace("", pd.NA).ffill()

# Extract only line-item rows
line_df = df[is_line].copy()

# Merge header info into each line item
for col in header_cols:
    line_df[col] = line_df[col]

# Add LineNo per Number
line_df["LineNo"] = line_df.groupby("Number").cumcount() + 1

# Combine item, description, and To Receive into a single column
line_df["LineDetail"] = line_df.apply(
    lambda x: f"{x['Item']} - {x['Description']} | To Receive: {x.get('To Receive','')}", axis=1
)

# Select final output columns
final_cols = [
    "Number",
    "LineNo",
    "LineDetail",
    "Ordered",
    "On Order",
    "Received"
]

line_df = line_df[final_cols]

# Save flattened CSV
line_df.to_csv(output_file, index=False)

print("Flattened line item file created:", output_file)
