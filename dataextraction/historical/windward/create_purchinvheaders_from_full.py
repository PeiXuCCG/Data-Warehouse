import pandas as pd

company = "tccg"
input_file = f"{company}/purchinvheader/purchases.csv"
output_file = f"{company}/purchinvheader/cleaned.csv"

# Load CSV
df = pd.read_csv(input_file, dtype=str, encoding='latin1').fillna("")





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



# Save the header-only output file
header_df.to_csv(output_file, index=False)

print("Header file created:", output_file)
