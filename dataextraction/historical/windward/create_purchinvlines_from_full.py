import pandas as pd

company = "tccg"
input_file = f"{company}/purchinvheader/purchases.csv"
output_file = f"{company}/purchinvheader/cleaned.csv"

# Read CSV with consistent empty strings
df = pd.read_csv(input_file, dtype=str, encoding='latin1').fillna("")

# Drop unwanted columns
df = df.drop(columns=["Total Stock Remaining", "PO Comment"])

# Add LineNo grouped by PO #
df["LineNo"] = df.groupby("PO #").cumcount() + 1

# Optional: reorder columns to put LineNo first
cols = ["PO #", "LineNo"] + [c for c in df.columns if c not in ["PO #", "LineNo"]]
df = df[cols]

# Save output
df.to_csv(output_file, index=False)

print("Line item file created with LineNo:", output_file)