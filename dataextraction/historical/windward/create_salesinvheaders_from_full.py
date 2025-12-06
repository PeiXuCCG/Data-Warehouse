import pandas as pd

company = "tccg"
input_file = f"{company}/salesinvline/sales.csv"
output_file = f"{company}/salesinvheader/cleaned.csv"

# Load CSV
# Load CSV
df = pd.read_csv(input_file, dtype=str).fillna("")

# Strip spaces from column names
df.columns = df.columns.str.strip().str.replace(" ", "")

# Move InvoiceNumber to the first column
cols = df.columns.tolist()
if "InvoiceNumber" in cols:
    cols.insert(0, cols.pop(cols.index("InvoiceNumber")))

# Keep only header-level fields (you can choose which fields to keep)
# For this example, we'll keep all columns except the line-specific ones like Parts, Labour, Tech Start, Tech End, etc.
# Adjust this list based on your logic
line_columns = ["Parts","Labour","TechStart","TechEnd","TechDesc","Serial"]
header_cols = [c for c in cols if c not in line_columns]

df_header = df[header_cols]

# Remove duplicate invoice numbers (optional)
df_header = df_header.drop_duplicates(subset=["InvoiceNumber"])

# Save the header file
df_header.to_csv(output_file, index=False)

print("Invoice header file created:", output_file)

