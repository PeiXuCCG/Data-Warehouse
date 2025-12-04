import pandas as pd

# ----------------------------
# Config
# ----------------------------
company = "complexrehab"
input_file = f"{company}/salesinvline/sales.csv"
output_file = f"{company}/salesinvline/cleaned_lines.csv"

# ----------------------------
# Load CSV
# ----------------------------
df = pd.read_csv(input_file, header=None, dtype=str).fillna("")

final_rows = []
current_invoice_row = None

# Define your desired headers here
headers = [
    "Sub Type","Number","Customer","Customer Acct","PO","Parts","Labour","SubTotal",
    "Taxes","Total","Cost","Profit","Margin","Invoice Date","Ordered Date",
    "Creation Date","Date of E","Date of W","Date of A","Invoice Number",
    "Invoice Ref No","Customer Part","Ship To","Delivery","Delivery Phone",
    "Delivery Email","Delivery Notes","Days to Pay","Balance Due","Salesperson",
    "","","Technicians","Tech Start","Tech End","Tech Desc","Rental State",
    "Rental Out","Rental In","Serial","Comments"
]

for i, row in df.iterrows():
    # Strip spaces from each column for checks
    row_stripped = [str(c).strip() for c in row]

    # Skip sub-header rows containing 'Part Number'
    if any("Part Number" in c for c in row_stripped):
        continue

    # Skip total rows
    if any(c.lower().startswith("total") for c in row_stripped if c):
        continue

    # Append the row to final_rows
    final_rows.append(row.tolist())

# Save final flattened CSV with headers
df = pd.DataFrame(final_rows, columns=headers)

columns_to_keep = [
    "Number","PO","Parts","Invoice Number","Invoice Ref No",
    "SubTotal","Taxes","Cost","Serial","Rental State","Rental Out","Rental In","Comments"
]

df_subset = df[columns_to_keep]

df_subset.to_csv(output_file, index=False)

print(f"Created flattened CSV: {output_file}")
