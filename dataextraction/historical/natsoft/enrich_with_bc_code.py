import pandas as pd
import glob

company = "mcleans"

ITEM_MAPPING_PATH = "../mapping/Item Mapping.csv"
ITEM_LIST = f"{company}/item/Item.csv"
OUTPUT_FILE = f"{company}/item/ItemList_with_BCCode.csv"

REAL_HEADERS = [
    "Item No",
    "Description",
    "SellUnit",
    "PurUnit",
    "GST",
    "Cost Price",
    "Buy Cost",
    "Sell Price",
    "Trade Price",
    "Supplier",
    "Department",
    "Group",
    "Track"
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
        left_on="Item No",
        right_on="LegacyCode",
        how="left"
    )
    merged["Item No"] = merged["BCCode"].fillna(merged["Item No"])

    merged = merged.drop(columns={"BCCode"})
    return merged


# RUN
item_list = load_csvs(ITEM_LIST)

item_mapping = load_csvs(ITEM_MAPPING_PATH)
item_mapping = item_mapping[item_mapping["company"] == " ".join(company.split("_")).title()]

df = update_item_list(item_list, item_mapping)



df.to_csv(OUTPUT_FILE, index=False)
print("Written:", OUTPUT_FILE)
