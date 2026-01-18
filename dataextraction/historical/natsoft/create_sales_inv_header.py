import pandas as pd

company = "mcleans"
input_file = f"{company}/salesinvoiceheader/SalesInvoices.csv"
output_file = f"{company}/salesinvoiceheader/cleaned.csv"

# Load CSV
df = pd.read_csv(input_file, dtype=str).fillna("")



# Required header fields
header_fields = [
"Date","Order ID","Customer","Name","Address","OrderNumber","SalesPerson","Area","Delivery","Total"
]

df = df[header_fields]

# Convert Cost Value to numeric
df["Total"] = pd.to_numeric(df["Total"], errors="coerce").fillna(0)

# Group by Order ID and sum Cost Value
header_df = (
    df.groupby(["Date","Order ID","Customer","Name","Address","OrderNumber","SalesPerson","Area","Delivery"], as_index=False)
      .agg({
             "Total": "sum"
      })
)

# Save the header-only output file
header_df.to_csv(output_file, index=False)

print("Header file created:", output_file)
