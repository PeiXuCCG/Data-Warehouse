import pandas as pd
import glob

company = "willaid" #ansteys, open_mobility

ITEM_MAPPING_PATH = "../mapping/Item Mapping.csv"
ITEM_LIST = f"{company}/item/sources/items.csv"
OUTPUT_FILE = f"{company}/item/ItemList_with_BCCode.csv"



#Open Mobility
REAL_HEADERS = [
    "ITEMCODE","WAREHOUSECODE","LOCATIONCODE","INVENTORYUNIT","INVENTORYQTY","AVERAGECOST","SERIALNO","ITEMCOLOUR"
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
        left_on="ITEMCODE",
        right_on="LegacyCode",
        how="left"
    )
    # Ensure BCCode exists
    if "BCCode" in merged.columns:
    # Replace empty strings or whitespace-only with NaN
        merged["BCCode"] = merged["BCCode"].replace(r"^\s*$", pd.NA, regex=True)
        
        # Coalesce: use BCCode if it has a value, else keep original sku
        merged["ITEMCODE"] = merged["BCCode"].combine_first(merged["ITEMCODE"])
        
        # Drop BCCode column
        merged = merged.drop(columns=["BCCode"])

    return merged


# RUN
item_list = load_csvs(ITEM_LIST)

item_mapping = load_csvs(ITEM_MAPPING_PATH)
item_mapping = item_mapping[item_mapping["company"] == " ".join(company.split("_")).title()]

df = update_item_list(item_list, item_mapping)



df.to_csv(OUTPUT_FILE, index=False)
print("Written:", OUTPUT_FILE)
