import pandas as pd

company = "tccg"
input_file = f"{company}/purchinvline/purchaseinvoices.csv"
output_file = f"{company}/purchinvheader/cleaned.csv"

# Load CSV
df = pd.read_csv(input_file, dtype=str, encoding='latin1').fillna("")

# Convert Ext Landed to numeric (remove $ and commas)
df["Ext Landed"] = (
    df["Ext Landed"]
    .str.replace("$", "", regex=False)
    .str.replace(",", "", regex=False)
    .astype(float)
)

# Sum Ext Landed per PO #
totals = df.groupby("PO #")["Ext Landed"].sum().reset_index()
totals = totals.rename(columns={"Ext Landed": "Total"})

# Required header fields
header_fields = [
    "PO #",
    "Description",
    "Date Received",
    "Date Expected",
    "PO Comment",
    "Supplier",
    "Department"
]

# Keep only required fields
header_df = df[header_fields].drop_duplicates(subset=["PO #"])

# Merge totals into header rows
header_df = header_df.merge(totals, on="PO #", how="left")

# Save the header-only output file
header_df.to_csv(output_file, index=False)

print("Header file created:", output_file)
