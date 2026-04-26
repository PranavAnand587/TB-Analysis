import time
import requests
import pandas as pd
from pathlib import Path

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

PAGE_SIZE = 1000
RETRY_LIMIT = 3
RETRY_DELAY = 2
REQUEST_DELAY = 0.3

OUTPUT_DIR = Path("data/raw")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

API_FILE = "api_links.txt"

NAME_MAP = {
    "1139": "TB_Notification",
    "1188": "TB_PatientCharacteristics",
    "1201": "TB_PresumptiveCases",
    "1221": "TB_Tobacco",
    "1227": "TB_Gender_Disaggregated_Outcomes",
    "1228": "TB_HIV_Coinfection",
    "1240": "TB_TreatmentOutcome_Public",
    "1241": "TB_TreatmentOutcome_Private",
    "1251": "TB_Diabetes_Coinfection",
    "1278": "TB_TreatmentOutcome_Total",
    "1279": "TB_TreatmentOutcome_New_Public",
    "1280": "TB_TreatmentOutcome_PreviouslyTreated_Public",
    "1372": "TB_Tribal",

    "7037": "RHS_Statewise",
    "7035": "RHS_Districtwise",

    "6821": "Health_Expenditure_Statewise",
    "6066": "Public_Health_Expenditure_By_Component",
    "2660": "Health_Expenditure_PerCapita",

    "7292": "NSS75_Reasons_NotUsingGovtHospital",
    "7298": "NSS75_OOP_Expenditure",
    "7290": "NSS75_Hospitalisation_By_Ailment",
    "7294": "NSS75_Treatment_Source",

    "I": "UNKNOWN"  # fallback safety (optional)
}

# ---------------------------------------------------------------------------
# LOAD DATASETS FROM FILE
# ---------------------------------------------------------------------------

import re

def extract_id(url: str):
    match = re.search(r"I(\d+)_", url)
    return match.group(1) if match else None


def load_datasets(file_path: str):
    datasets = []

    with open(file_path, "r") as f:
        content = f.read()

    blocks = content.strip().split("\n\n")

    for i, block in enumerate(blocks):
        url = block.strip()
        if not url:
            continue

        base_url = url.replace("&pageno=1", "")

        dataset_id = extract_id(url)
        name = NAME_MAP.get(dataset_id, f"dataset_{i+1}")

        datasets.append({
            "name": name,
            "url": base_url
        })

    return datasets

def extract_indicator_map(result_json):
    mapping = {}

    headers_obj = result_json.get("Headers")

    if not headers_obj:
        print("DEBUG: No Headers found")
        return mapping

    headers = headers_obj.get("Items", [])

    print(f"DEBUG: Headers count = {len(headers)}")

    for h in headers:
        ind_dim = str(h.get("indicator_dimension", "")).lower()

        if ind_dim in ["indicator", "measure", "measures"]:
            ind_id = h.get("ID")
            name = h.get("DisplayName")

            if ind_id and name:
                clean_name = (
                    name.strip()
                    .replace(" ", "_")
                    .replace("(", "")
                    .replace(")", "")
                    .replace("/", "_")
                )
                mapping[ind_id] = clean_name

    print(f"DEBUG: Extracted {len(mapping)} indicators")
    return mapping

def apply_indicator_names(df, mapping):
    new_cols = {}

    for col in df.columns:
        base = col.replace("_sum", "").replace("_avg", "")

        if base in mapping:
            new_name = mapping[base]

            if col.endswith("_sum"):
                new_name += "_sum"
            elif col.endswith("_avg"):
                new_name += "_avg"

            new_cols[col] = new_name

    return df.rename(columns=new_cols)

# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def fetch_page(base_url: str, page: int):
    url = f"{base_url}&pageno={page}"

    for attempt in range(1, RETRY_LIMIT + 1):
        try:
            resp = requests.get(url, timeout=60)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            print(f"    [attempt {attempt}/{RETRY_LIMIT}] Error on page {page}: {e}")
            if attempt < RETRY_LIMIT:
                time.sleep(RETRY_DELAY * attempt)

    return None


def extract_rows(data_list: list):
    rows = []
    for item in data_list:
        row = {}
        for key, val in item.items():
            if isinstance(val, dict):
                row[f"{key}_sum"] = val.get("sum")
                row[f"{key}_avg"] = val.get("avg")
            else:
                row[key] = val
        rows.append(row)
    return rows

def pull_dataset(dataset: dict):
    name = dataset["name"]
    base_url = dataset["url"]

    print(f"\n{'='*60}")
    print(f"Pulling: {name}")

    all_rows = []
    page = 1
    indicator_map = None

    while True:
        print(f"  page {page}...", end=" ", flush=True)

        result = fetch_page(base_url, page)

        if result is None:
            print("FAILED — skipping")
            break

        # Extract metadata ONLY once
        if indicator_map is None:
            indicator_map = extract_indicator_map(result)

        data_list = result.get("Data", [])

        if not data_list:
            print("empty — done")
            break

        rows = extract_rows(data_list)
        all_rows.extend(rows)

        print(f"got {len(rows)} rows (total: {len(all_rows)})")

        if len(data_list) < PAGE_SIZE:
            break

        page += 1
        time.sleep(REQUEST_DELAY)

    if not all_rows:
        print(f"  WARNING: no data for {name}")
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)

    # APPLY RENAMING HERE
    if indicator_map:
        df = apply_indicator_names(df, indicator_map)

    print(f"  Final shape: {df.shape}")
    return df

def save_splits(df: pd.DataFrame, name: str):
    if df.empty:
        print(f"  Skipping {name} — empty")
        return

    # ALWAYS save raw
    raw_path = OUTPUT_DIR / f"{name}_raw.csv"
    df.to_csv(raw_path, index=False)
    print(f"  Saved raw → {raw_path} ({len(df)} rows)")

    has_district = "DistrictCode" in df.columns
    has_state = "StateCode" in df.columns

    # District
    if has_district:
        district_df = df[df["DistrictCode"].notna() & (df["DistrictCode"] != "")]
        if not district_df.empty:
            path = OUTPUT_DIR / f"{name}_district.csv"
            district_df.to_csv(path, index=False)
            print(f"  Saved district → {path}")

    # State
    if has_state:
        if has_district:
            state_df = df[df["DistrictCode"].isna() | (df["DistrictCode"] == "")]
        else:
            state_df = df.copy()

        if not state_df.empty:
            path = OUTPUT_DIR / f"{name}_state.csv"
            state_df.to_csv(path, index=False)
            print(f"  Saved state → {path}")

    # National
    if has_state:
        nat_df = df[df["StateCode"].isna() | (df["StateCode"] == "")]
        if not nat_df.empty:
            path = OUTPUT_DIR / f"{name}_national.csv"
            nat_df.to_csv(path, index=False)
            print(f"  Saved national → {path}")


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    print("NDAP Data Collection\n")

    datasets = load_datasets(API_FILE)

    summary = []

    for dataset in datasets:
        df = pull_dataset(dataset)
        save_splits(df, dataset["name"])

        summary.append({
            "name": dataset["name"],
            "rows": len(df),
            "cols": len(df.columns) if not df.empty else 0
        })

    summary_df = pd.DataFrame(summary)
    print("\nSUMMARY:\n", summary_df)


if __name__ == "__main__":
    main()