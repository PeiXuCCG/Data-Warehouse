import pandas as pd
import glob

import pandas as pd

# ----------------------------------------------------
# 1. LOAD FILES
# ----------------------------------------------------
# Replace these with your actual file paths

company = "willaid"
invoices_path = "header.csv"
payments_path = "PaymentTransactions.csv"

df_inv = pd.read_csv(f"{company}/custledger/{invoices_path}")
df_pay = pd.read_csv(f"{company}/custledger/{payments_path}")

# ----------------------------------------------------
# 2. NORMALIZE COMMON COLUMN NAMES
# ----------------------------------------------------
df_inv = df_inv.rename(columns={
    "INVOICENUMBER": "DocumentNumber",
    "INVOICEDATE": "DocumentDate",
    "CUSTOMER": "Customer",
    "INVOICETOTALAMOUNT": "DebitAmount",     # Invoice total → Debit
    "BALANCEDUE": "BalanceDue"
})

df_pay = df_pay.rename(columns={
    "PAYMENTNUMBER": "DocumentNumber",
    "PAYMENTDATE": "DocumentDate",
    "CUSTOMER": "Customer",
    "PAYMENTAMOUNT": "CreditAmount",         # Payment → Credit
    "INVOICENUMBER": "AppliedInvoice"
})

# ----------------------------------------------------
# 3. BUILD INVOICE LEDGER ENTRIES (DEBITS)
# ----------------------------------------------------
invoice_ledger = pd.DataFrame({
    "Customer": df_inv["Customer"],
    "DocumentNumber": df_inv["DocumentNumber"],
    "DocumentType": "INVOICE",
    "DocumentDate": df_inv["DocumentDate"],
    "Debit": df_inv["DebitAmount"],
    "Credit": 0.0,
    "Notes": df_inv.get("INVOICENOTES", "")
})

# ----------------------------------------------------
# 4. BUILD PAYMENT LEDGER ENTRIES (CREDITS)
# ----------------------------------------------------
payment_ledger = pd.DataFrame({
    "Customer": df_pay["Customer"],
    "DocumentNumber": df_pay["DocumentNumber"],
    "DocumentType": "PAYMENT",
    "DocumentDate": df_pay["DocumentDate"],
    "Debit": 0.0,
    "Credit": df_pay["CreditAmount"],
    "Notes": df_pay.get("AppliedInvoice", "")
})

# ----------------------------------------------------
# 5. COMBINE INTO ONE LEDGER
# ----------------------------------------------------
ledger = pd.concat([invoice_ledger, payment_ledger], ignore_index=True)

# Sort ledger by Customer + Date
ledger = ledger.sort_values(["Customer", "DocumentDate"]).reset_index(drop=True)



# ----------------------------------------------------
# 7. SAVE OUTPUT
# ----------------------------------------------------
ledger.to_csv("customer_ledger.csv", index=False)
print("Ledger created: combined_ledger.csv")

