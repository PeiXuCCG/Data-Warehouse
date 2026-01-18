import pandas as pd
import os

company = "fisherlane"

OUTPUT_FILE = f"{company}/purchinvheader/joined.csv"

# ---- LOAD YOUR FILES ----
purchases = pd.read_csv(f"{company}/purchinvheader/header.csv")
invoices = pd.read_csv(f"{company}/purchinvheader/invoices.csv")



purch_header = purchases.merge(
    invoices,
    on="PO_delivery_id",     # join key
    how="left"         # keep all sales lines
)


# -----------------------------
# 7. SAVE OUTPUT
# -----------------------------

purch_header.to_csv(OUTPUT_FILE, index=False)

print(f"Item Ledger Entry written to {OUTPUT_FILE}")
