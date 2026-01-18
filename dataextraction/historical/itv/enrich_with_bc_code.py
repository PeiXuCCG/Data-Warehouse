import pandas as pd
import glob

company = "fisherlane"

ITEM_MAPPING_PATH = "../mapping/Item Mapping.csv"
ITEM_LIST = f"{company}/item/item.csv"
OUTPUT_FILE = f"{company}/item/ItemList_with_BCCode.csv"

REAL_HEADERS = [
    "code",
    "Supplier_Code",
    "BCCode",
    "Supplier_Id",
    "CCGVendor",
    "supplier_name",
    "department_name",
    "sub_department_name",
    "sub_sub_department_name",
    "Description_First",
    "Tax_Abbrev",
    "archived",
    "Discontinued",
    "Last_Cost_Ex",
    "price_inc_1",
    "min_soh",
    "max_soh",
    "SOH"
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
        left_on="BCCode",
        right_on="BCCode",
        how="left"
    )
   
    return merged


# RUN
item_list = load_csvs(ITEM_LIST)

item_mapping = load_csvs(ITEM_MAPPING_PATH)
item_mapping = item_mapping[item_mapping["company"] == " ".join(company.split("_")).title()]

df = update_item_list(item_list, item_mapping)



df.to_csv(OUTPUT_FILE, index=False)
print("Written:", OUTPUT_FILE)
