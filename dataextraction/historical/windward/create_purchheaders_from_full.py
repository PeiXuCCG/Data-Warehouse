import pandas as pd

company = "countrycaremelbourne"
input_file = f"{company}/purchheader/purchase_30Jun.csv"
output_file = f"{company}/purchheader/cleaned2.csv"

# Read CSV
df = pd.read_csv(input_file, dtype=str, encoding='latin1')

# Rename "Ordered" → "OrderedOn"
df = df.rename(columns={"Ordered": "OrderedOn"})

# Keep only header rows (rows where Number is not empty)
header_df = df[df["Number"].notna() & (df["Number"] != "")]

# Select required columns
header_df = header_df[["Number", "Date", "Supplier", "OrderedOn"]]

# Drop duplicate Numbers
header_df = header_df.drop_duplicates(subset=["Number"])

# Save output
header_df.to_csv(output_file, index=False)

print("Header file created:", output_file)
