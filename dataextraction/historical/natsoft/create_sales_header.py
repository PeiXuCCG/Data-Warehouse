import pandas as pd

company = "mcleans"
input_file = f"{company}/salesheader/salesheader.csv"
output_file = f"{company}/salesheader/cleaned.csv"

# Load CSV
df = pd.read_csv(input_file, dtype=str).fillna("")

# Required header fields
header_fields = [
"Order ID","Date","Customer","Name","Sales Value","Cost Value"
]

df = df[header_fields]

# Convert Cost Value to numeric
df["Cost Value"] = pd.to_numeric(df["Cost Value"], errors="coerce").fillna(0)
df["Sales Value"] = pd.to_numeric(df["Sales Value"], errors="coerce").fillna(0)

print(df)

# Group by Order ID and sum Cost Value
header_df = (
    df.groupby(["Order ID", "Date", "Customer", "Name"], as_index=False)
      .agg({
          "Cost Value": "sum",
          "Sales Value": "sum"
      })
)

header_df = header_df.rename(columns={"Cost Value": "Total Cost"})
header_df = header_df.rename(columns={"Sale Value": "Total"})

# Save the header-only output file
header_df.to_csv(output_file, index=False)

print("Header file created:", output_file)
