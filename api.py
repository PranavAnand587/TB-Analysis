import requests
import json

API_KEY = "gAAAAABp6KnuCASI6UpYXxLzm8RAtrKxKLXarivwaPgrvKBF4NqH8KtROpAPkKhuHbJuR2jkTiS4eHm1owjU-TZ0a1cu1XGCbjtPqlWuCPFHIc1gzwjm76Vc7InFiN9Q4B-WLQwNWYmGyWy2DcIuMgSL-4M-yV2AZUkyckMLeaQKjwqmfeH7UBhYIvbU1iyN1v0QB47RUcbhNHzqmPDhPznS1Y7YjE99Cw=="

"""
NDAP Data Collection Script — DSM Project
==========================================
Pulls all datasets defined in DATASETS config, paginates through all pages,
and saves both district-level (raw) and state-level (aggregated) CSVs to ./data/

Usage:
    python ndap_collect.py

Output:
    data/raw/<source_code>_district.csv   — one row per district × year (× dimension)
    data/raw/<source_code>_state.csv      — one row per state × year (× dimension)
    data/raw/<source_code>_national.csv   — one row per year (× dimension), where applicable
"""

import os
import time
import requests
import pandas as pd
from pathlib import Path

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

BASE_URL = "https://loadqa.ndapapi.com/v1/openapi"
PAGE_SIZE = 1000         # API returns up to 1000 rows per page
RETRY_LIMIT = 3
RETRY_DELAY = 2          # seconds between retries
REQUEST_DELAY = 0.3      # seconds between normal requests (be polite)

OUTPUT_DIR = Path("data/raw")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# DATASET DEFINITIONS
# (source_code, name, indicators, extra_dimensions)
# Indicators are the I<code>_<n> columns. Extra dims are dataset-specific
# categorical dimensions beyond the standard location/time dims.
# ---------------------------------------------------------------------------


DATASETS = [
    # ── TB Report ──────────────────────────────────────────────────────────
    {
        "source_code": 1139,
        "name": "TB_Notification",
        "indicators": ["I1139_5", "I1139_6", "I1139_7", "I1139_8", "I1139_9", "I1139_10"],
        "dims": ["Country", "StateName", "StateCode", "DistrictName", "DistrictCode", "Year","D1139_4"],   # Sector (Public/Private/Total)
    },
    {
        "source_code": 1188,
        "name": "TB_PatientCharacteristics",
        "indicators": ["I1188_5", "I1188_6", "I1188_7", "I1188_8", "I1188_9", "I1188_10"],
        "dims": [],
    },
    {
        "source_code": 1228,
        "name": "TB_HIV_Coinfection",
        "indicators": ["I1228_5", "I1228_6", "I1228_7", "I1228_8"],
        "dims": [],
    },
    {
        "source_code": 1240,
        "name": "TB_TreatmentOutcome_Public",
        "indicators": ["I1240_5", "I1240_6", "I1240_7", "I1240_8", "I1240_9", "I1240_10"],
        "dims": [],
    },
    {
        "source_code": 1241,
        "name": "TB_TreatmentOutcome_Private",
        "indicators": ["I1241_5", "I1241_6", "I1241_7", "I1241_8", "I1241_9", "I1241_10"],
        "dims": [],
    },
    {
        "source_code": 1251,
        "name": "TB_Diabetes_Coinfection",
        "indicators": ["I1251_5", "I1251_6", "I1251_7", "I1251_8"],
        "dims": [],
    },
    {
        "source_code": 1278,
        "name": "TB_TreatmentOutcome_Total",
        "indicators": ["I1278_5", "I1278_6", "I1278_7", "I1278_8", "I1278_9", "I1278_10"],
        "dims": [],
    },
    {
        "source_code": 1372,
        "name": "TB_Tribal",
        "indicators": ["I1372_5", "I1372_6", "I1372_7", "I1372_8"],
        "dims": [],
    },
    # ── Rural Health Statistics ─────────────────────────────────────────────
    {
        "source_code": 7037,
        "name": "RHS_Statewise",
        "indicators": [f"I7037_{i}" for i in range(5, 55)],   # broad sweep; prune later
        "dims": [],
    },
    {
        "source_code": 7035,
        "name": "RHS_Districtwise",
        "indicators": [f"I7035_{i}" for i in range(5, 30)],
        "dims": [],
    },
    # ── NSS Round 75 ───────────────────────────────────────────────────────
    {
        "source_code": 7292,
        "name": "NSS75_ReasonNotUsingGovtHospital",
        "indicators": [f"I7292_{i}" for i in range(5, 20)],
        "dims": [],
    },
    {
        "source_code": 7298,
        "name": "NSS75_OOP_Expenditure",
        "indicators": [f"I7298_{i}" for i in range(5, 20)],
        "dims": [],
    },
]

# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def fetch_page(source_code: int, indicators: list, dims: list, page: int) -> dict | None:
    """Fetch a single page from the NDAP API. Returns parsed JSON or None on failure.
    
    NOTE: Uses raw URL string construction — the API requires literal commas in
    ind= and dim= parameters. requests' params dict percent-encodes commas (%2C)
    which causes the API to return empty Data.
    """
    url = (
        f"{BASE_URL}"
        f"?API_Key={API_KEY}"
        f"&ind={','.join(indicators)}"
        f"&dim={','.join(dims)}"
        f"&pageno={page}"
    )
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


def extract_rows(data_list: list, indicators: list) -> list[dict]:
    """
    Flatten one page's Data array into a list of plain dicts.
    Each indicator object → two columns: <ind>_sum and <ind>_avg.
    """
    rows = []
    for item in data_list:
        row = {}
        for key, val in item.items():
            if isinstance(val, dict):
                # indicator object — extract sum and avg
                row[f"{key}_sum"] = val.get("sum")
                row[f"{key}_avg"] = val.get("avg")
            else:
                row[key] = val
        rows.append(row)
    return rows


def pull_dataset(dataset: dict) -> pd.DataFrame:
    """Pull all pages for a dataset and return a combined DataFrame."""
    source_code = dataset["source_code"]
    name = dataset["name"]
    indicators = dataset["indicators"]
    dims = dataset["dims"]

    print(f"\n{'='*60}")
    print(f"Pulling: {name} (source_code={source_code})")
    print(f"  indicators : {indicators}")
    print(f"  dims       : {dims}")

    all_rows = []
    page = 1

    while True:
        print(f"  page {page}...", end=" ", flush=True)
        result = fetch_page(source_code, indicators, dims, page)

        if result is None:
            print("FAILED — skipping remaining pages")
            break

        data_list = result.get("Data", [])   # top-level key, not result["Source"]["Data"]

        if not data_list:
            print("empty — done")
            break

        rows = extract_rows(data_list, indicators)
        all_rows.extend(rows)
        print(f"got {len(rows)} rows (total so far: {len(all_rows)})")

        # Stop if this page returned fewer rows than a full page
        if len(data_list) < PAGE_SIZE:
            break

        page += 1
        time.sleep(REQUEST_DELAY)

    if not all_rows:
        print(f"  WARNING: no data collected for {name}")
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    print(f"  Final shape: {df.shape}")
    return df


def save_splits(df: pd.DataFrame, name: str):
    """
    Save three splits of the dataframe:
      - district level: all rows (DistrictCode present and not null)
      - state level:    rows where DistrictCode is null/missing (state aggregates)
                        OR aggregated from district rows grouped by state
      - national level: rows where StateCode is null/missing
    """
    if df.empty:
        print(f"  Skipping save for {name} — empty dataframe")
        return

    # Normalise column presence
    has_district = "DistrictCode" in df.columns
    has_state = "StateCode" in df.columns

    # ── District-level: rows that have a DistrictCode ──
    if has_district:
        district_df = df[df["DistrictCode"].notna() & (df["DistrictCode"] != "")]
        if not district_df.empty:
            path = OUTPUT_DIR / f"{name}_district.csv"
            district_df.to_csv(path, index=False)
            print(f"  Saved district-level  → {path}  ({len(district_df)} rows)")

    # ── State-level: rows without a DistrictCode (pre-aggregated by API) ──
    if has_district and has_state:
        state_df = df[df["DistrictCode"].isna() | (df["DistrictCode"] == "")]
        # If the API doesn't return pre-aggregated state rows, aggregate ourselves
        if state_df.empty:
            print(f"  No pre-aggregated state rows found — computing state aggregates")
            num_cols = [c for c in df.columns if c.endswith("_sum") or c.endswith("_avg")]
            id_cols = [c for c in ["Country", "StateName", "StateCode", "Year"] + 
                       [c for c in df.columns if c.startswith("D")]
                       if c in df.columns]
            agg_dict = {c: "sum" if c.endswith("_sum") else "mean" for c in num_cols}
            state_df = df.groupby(id_cols, as_index=False).agg(agg_dict)

        if not state_df.empty:
            path = OUTPUT_DIR / f"{name}_state.csv"
            state_df.to_csv(path, index=False)
            print(f"  Saved state-level     → {path}  ({len(state_df)} rows)")

    # ── National: rows without StateCode ──
    if has_state:
        nat_df = df[df["StateCode"].isna() | (df["StateCode"] == "")]
        if not nat_df.empty:
            path = OUTPUT_DIR / f"{name}_national.csv"
            nat_df.to_csv(path, index=False)
            print(f"  Saved national-level  → {path}  ({len(nat_df)} rows)")


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    print("NDAP Data Collection")
    print(f"Output directory: {OUTPUT_DIR.resolve()}\n")

    summary = []

    for dataset in DATASETS:
        df = pull_dataset(dataset)
        save_splits(df, dataset["name"])
        summary.append({
            "name": dataset["name"],
            "source_code": dataset["source_code"],
            "rows_collected": len(df),
            "columns": len(df.columns) if not df.empty else 0,
        })

    print("\n\n" + "="*60)
    print("COLLECTION SUMMARY")
    print("="*60)
    summary_df = pd.DataFrame(summary)
    print(summary_df.to_string(index=False))

    summary_path = OUTPUT_DIR / "_collection_summary.csv"
    summary_df.to_csv(summary_path, index=False)
    print(f"\nSummary saved to {summary_path}")


if __name__ == "__main__":
    main()