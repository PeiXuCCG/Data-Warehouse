import pandas as pd

company = "mcleans"
input_file = f"{company}/purchheader/PurchaseOrders.csv"
output_file = f"{company}/purchheader/cleaned.csv"

# Load CSV
df = pd.read_csv(input_file, dtype=str).fillna("")


print(df)


# Required header fields
header_fields = [
    "OrderID",
    "Date",
    "Supplier",
    "Name",
    "Cost Value"
]

df = df[header_fields]

# Convert Cost Value to numeric
df["Cost Value"] = pd.to_numeric(df["Cost Value"], errors="coerce").fillna(0)

# Group by Order ID and sum Cost Value
header_df = (
    df.groupby(["OrderID", "Date", "Supplier", "Name"], as_index=False)
      .agg({"Cost Value": "sum"})
)

header_df = header_df.rename(columns={"Cost Value": "Total"})

# Save the header-only output file
header_df.to_csv(output_file, index=False)

print("Header file created:", output_file)
