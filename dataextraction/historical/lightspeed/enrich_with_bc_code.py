import pandas as pd
import glob

company = "open_mobility" #ansteys, open_mobility

ITEM_MAPPING_PATH = "../mapping/Item Mapping.csv"
ITEM_LIST = f"{company}/item/sources/product-export.csv"
OUTPUT_FILE = f"{company}/item/ItemList_with_BCCode.csv"

# Ansteys
# REAL_HEADERS = [
#     "id",
#     "handle",
#     "sku",
#     "composite_name",
#     "composite_sku",
#     "composite_quantity",
#     "name",
#     "description",
#     "product_category",
#     "variant_option_one_name",
#     "variant_option_one_value",
#     "variant_option_two_name",
#     "variant_option_two_value",
#     "variant_option_three_name",
#     "variant_option_three_value",
#     "tags",
#     "active_online",
#     "weight",
#     "weight_unit",
#     "length",
#     "width",
#     "height",
#     "dimensions_unit",
#     "supply_price",
#     "retail_price",
#     "tax_name",
#     "tax_value",
#     "account_code",
#     "account_code_purchase",
#     "brand_name",
#     "supplier_name",
#     "supplier_code",
#     "active",
#     "track_inventory",
#     "inventory_Ansteys_Broadmeadow",
#     "reorder_point_Ansteys_Broadmeadow",
#     "restock_level_Ansteys_Broadmeadow",
#     "inventory_Ansteys_Maitland",
#     "reorder_point_Ansteys_Maitland",
#     "restock_level_Ansteys_Maitland",
#     "inventory_Ansteys_Salamander_Bay",
#     "reorder_point_Ansteys_Salamander_Bay",
#     "restock_level_Ansteys_Salamander_Bay",
#     "inventory_CCG_Newcastle",
#     "reorder_point_CCG_Newcastle",
#     "restock_level_CCG_Newcastle"


# ]

#Open Mobility
REAL_HEADERS = [
    "id",
    "handle",
    "sku",
    "composite_name",
    "composite_sku",
    "composite_quantity",
    "name",
    "description",
    "product_category",
    "variant_option_one_name",
    "variant_option_one_value",
    "variant_option_two_name",
    "variant_option_two_value",
    "variant_option_three_name",
    "variant_option_three_value",
    "tags",
    "active_online",
    "weight",
    "weight_unit",
    "length",
    "width",
    "height",
    "dimensions_unit",
    "supply_price",
    "retail_price",
    "tax_name",
    "tax_value",
    "account_code",
    "account_code_purchase",
    "brand_name",
    "supplier_name",
    "supplier_code",
    "active",
    "track_inventory",
    "inventory_WGA_Pearson",
    "reorder_point_WGA_Pearson",
    "restock_level_WGA_Pearson",
    "inventory_Warehouse_-_Pearson_WAGGA_WAGGA_(MAIN)",
    "reorder_point_Warehouse_-_Pearson_WAGGA_WAGGA_(MAIN)",
    "restock_level_Warehouse_-_Pearson_WAGGA_WAGGA_(MAIN)"
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
        left_on="sku",
        right_on="LegacyCode",
        how="left"
    )
    # Ensure BCCode exists
    if "BCCode" in merged.columns:
    # Replace empty strings or whitespace-only with NaN
        merged["BCCode"] = merged["BCCode"].replace(r"^\s*$", pd.NA, regex=True)
        
        # Coalesce: use BCCode if it has a value, else keep original sku
        merged["sku"] = merged["BCCode"].combine_first(merged["sku"])
        
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
