import pandas as pd
import glob

company = "healthsaver"
SALES_PATH = f"{company}/purchinvline/cleaned_2.csv"
OUTPUT_FILE = f"{company}/purchinvline/cleaned_2.csv"


def load_csvs(path):
    files = glob.glob(path)
    dfs = []
    for f in files:
        df = pd.read_csv(f, header=0, dtype=str, keep_default_na=False)


        dfs.append(df)

    return pd.concat(dfs, ignore_index=True)

def clean_sales(df):
    df = df.dropna(how="all")
    df = df[~(df.eq("").all(axis=1))]
    return df

# RUN
sl = load_csvs(SALES_PATH)


cleaned = clean_sales(sl)
cleaned.to_csv(OUTPUT_FILE, index=False)

print("Written:", OUTPUT_FILE)
