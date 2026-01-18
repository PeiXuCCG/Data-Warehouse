import pandas as pd
import os

company = "fisherlane"


# ---- LOAD YOUR FILES ----
header = pd.read_csv(f"{company}/salesinvoiceheader/header.csv")
line = pd.read_csv(f"{company}/salesinvoiceline/lines.csv")


header_txn_ids = set(header["transaction_id"].dropna().unique())
line_txn_ids = set(line["transaction_id"].dropna().unique())

headers_without_lines = header_txn_ids - line_txn_ids

lines_without_headers = line_txn_ids - header_txn_ids

print(f"Headers without lines: {len(headers_without_lines)}")
print(f"Lines without headers: {len(lines_without_headers)}")

if headers_without_lines:
    print("Missing lines for transaction_ids:")
    for r in headers_without_lines:
        print(r)
  

if lines_without_headers:
    print("Orphan lines with no header:")
    print(sorted(lines_without_headers))