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


# ---------------------------------------------------------------------------
# LOAD DATASETS FROM FILE
# ---------------------------------------------------------------------------

def load_datasets(file_path: str):
    datasets = []
    with open(file_path, "r") as f:
        content = f.read()

    blocks = content.strip().split("\n\n")

    for i, block in enumerate(blocks):
        url = block.strip()

        if not url:
            continue

        # remove pageno=1 so we can paginate
        base_url = url.replace("&pageno=1", "")

        datasets.append({
            "name": f"dataset_{i+1}",
            "url": base_url
        })

    return datasets


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

    while True:
        print(f"  page {page}...", end=" ", flush=True)

        result = fetch_page(base_url, page)

        if result is None:
            print("FAILED — skipping")
            break

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