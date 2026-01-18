import pandas as pd
import glob

company = "lakeside_mobility"

ITEM_MAPPING_PATH = "../mapping/Item Mapping.csv"
ITEM_LIST = f"{company}/item/ItemsList.csv"
OUTPUT_FILE = f"{company}/item/ItemList_with_BCCode.csv"

REAL_HEADERS = [
    "Branch Name","Item Type","Category","Category 2","Group Code","LegacyCode",
    "Description","Serial Number","Supplier Code","Barcode","Quantity For Hire",
    "Make","Model","Rego","Prompt","Returns Prompt","Attachment","Attachment 2",
    "Colour","Height","Width","Depth","Length","Area","Volume","Weight",
    "Location Slot","Preferred Supplier","Cost Price Ex","Cost Price Tax",
    "Cost Price","Landed Cost","Purchase Date","Disposal Date",
    "Finance Repayment Amount","Finance Start Date","Finance Finish Date",
    "Service Interval","Service Interval 2",
    "Custom Field 1","Custom Field 2","Custom Field 3","Custom Field 4",
    "Custom Field 5","Custom Field 6","Custom Field 7","Custom Field 8",
    "Photo","Photo 2","Photo 3","Photo 4","Photo 5","Photo 6",
    "Non Stock","Is Delivery Vehicle","Metered","Package Header",
    "Test Tag Not Required","EOM RollOver Item",
    "Restrict DoubleBookings","Serial Required"
]

ITEM_MAPPING_HEADERS = [
    "LegacyCode","BCCode","Desc Names","company","Source_system"
]


def load_csvs(path):
    files = glob.glob(path)
    dfs = []
    for f in files:
        df = pd.read_csv(f, dtype=str, keep_default_na=False)
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True)


def update_item_list(item_list, item_mapping):
    merged = pd.merge(
        item_list,
        item_mapping[["LegacyCode", "BCCode"]],
        left_on="Supplier Code",
        right_on="LegacyCode",
        how="left"
    )
    merged = merged.rename(columns={"BCCode": "Code"})
    return merged


# RUN
item_list = load_csvs(ITEM_LIST)

item_mapping = load_csvs(ITEM_MAPPING_PATH)
item_mapping = item_mapping[item_mapping["company"] == " ".join(company.split("_")).title()]

cleaned = update_item_list(item_list, item_mapping)

df = (
    cleaned.drop(columns=["LegacyCode_y"], errors="ignore")
      .rename(columns={"LegacyCode_x": "LegacyCode"})
)

df.to_csv(OUTPUT_FILE, index=False)
print("Written:", OUTPUT_FILE)
