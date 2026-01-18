import pandas as pd

company = "mcleans"
input_file = f"{company}/purchinvheader/Excel Export_PurchaseInvoices.csv"
output_file = f"{company}/purchinvheader/cleaned.csv"

# Load CSV
df = pd.read_csv(input_file, dtype=str).fillna("")



# Required header fields
header_fields = [
"Date","Reference","Supplier","Name","Address","OrderNumber","Tax","Total"
]

df = df[header_fields]

# Convert Cost Value to numeric
df["Total"] = pd.to_numeric(df["Total"], errors="coerce").fillna(0)
df["Tax"] = pd.to_numeric(df["Tax"], errors="coerce").fillna(0)

# Group by Order ID and sum Cost Value
header_df = (
    df.groupby(["Date","Reference","Supplier","Name","Address","OrderNumber"], as_index=False)
      .agg({
             "Total": "sum",
             "Tax": "sum"
      })
)

# Save the header-only output file
header_df.to_csv(output_file, index=False)

print("Header file created:", output_file)
