import requests
import json

API_KEY = "gAAAAABp6K7Xjz_rC8LZBPoKTVurCoymBzgwi-Rm7xqd57J9U33Pb9GpSUaQCeH05bmNnn_eipTajcBMH4RFziV9yVbjPCt7-CO8J_iTSEHE1LhXho8hi63FI9-GGhwXh7XRc2F_UHF3S9Qe-6tD_SVgcsEMVY7l83rfR5mJ3AjJ3e2cHCXjpJ6BocYN8YltFTArx1l2IYGCvnK4gXwJ3wprTy8FVUNdXQ=="
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
        "indicators": ["I1188_3", "I1188_4", "I1188_5", "I1188_6", "I1188_7", "I1188_8", "I1188_9", "I1188_10", "I1188_11", "I1188_12", "I1188_13", "I1188_14", "I1188_15", "I1188_16", "I1188_17", "I1188_18", "I1188_19", "I1188_20", "I1188_21", "I1188_22", "I1188_23", "I1188_24", "I1188_25", "I1188_26", "I1188_27", "I1188_28", "I1188_29"],
        "dims": ["Country", "StateName", "StateCode", "Year"],
    },
    {
        "source_code": 1228,
        "name": "TB_HIV_Coinfection",
        "indicators": ["I1228_3", "I1228_4", "I1228_5", "I1228_6", "I1228_7", "I1228_8", "I1228_9", "I1228_10", "I1228_11", "I1228_12", "I1228_13", "I1228_14"],
        "dims": ["Country", "StateName", "StateCode", "Year"],
    },
    {
        "source_code": 1240,
        "name": "TB_TreatmentOutcome_Public",
        "indicators": ["I1240_3", "I1240_4", "I1240_5", "I1240_6", "I1240_7", "I1240_8", "I1240_9", "I1240_10", "I1240_11", "I1240_12", "I1240_13", "I1240_14", "I1240_15", "I1240_16", "I1240_17", "I1240_18"],
        "dims": ["Country", "StateName", "StateCode", "Year"],
    },
    {
        "source_code": 1241,
        "name": "TB_TreatmentOutcome_Private",
        "indicators": ["I1241_3", "I1241_4", "I1241_5", "I1241_6", "I1241_7", "I1241_8", "I1241_9", "I1241_10", "I1241_11", "I1241_12", "I1241_13", "I1241_14", "I1241_15", "I1241_16", "I1241_17", "I1241_18", "I1241_19"],
        "dims": ["Country", "StateName", "StateCode", "Year"],
    },
    {
        "source_code": 1251,
        "name": "TB_Diabetes_Coinfection",
        "indicators": ["I1251_3", "I1251_11", "I1251_4", "I1251_12", "I1251_5", "I1251_13", "I1251_6", "I1251_14", "I1251_7", "I1251_15", "I1251_8", "I1251_16", "I1251_9", "I1251_10"],
        "dims": ["Country", "StateName", "StateCode", "Year"],
    },
    {
        "source_code": 1278,
        "name": "TB_TreatmentOutcome_Total",
        "indicators": ["I1278_3", "I1278_4", "I1278_5", "I1278_6", "I1278_7", "I1278_8", "I1278_9", "I1278_10", "I1278_11", "I1278_12", "I1278_13", "I1278_14", "I1278_15", "I1278_16", "I1278_17", "I1278_18"],
        "dims": ["Country", "StateName", "StateCode", "Year"],
    },
    {
        "source_code": 1372,
        "name": "TB_Tribal",
        "indicators": ["I1372_3", "I1372_4", "I1372_5", "I1372_6", "I1372_7", "I1372_8", "I1372_9", "I1372_10", "I1372_11", "I1372_12", "I1372_13", "I1372_14", "I1372_15", "I1372_16", "I1372_17"],
        "dims": ["Country", "StateName", "StateCode", "Year"],
    },
    # ── Rural Health Statistics ─────────────────────────────────────────────
    {
        "source_code": 7037,
        "name": "RHS_Statewise",
        "indicators": ["I7037_6", "I7037_7", "I7037_4", "I7037_3", "I7037_5", "I7037_8", "I7037_9", "I7037_10", "I7037_11", "I7037_12", "I7037_13", "I7037_14", "I7037_15", "I7037_16", "I7037_17", "I7037_18", "I7037_19", "I7037_20", "I7037_21", "I7037_22", "I7037_23", "I7037_24", "I7037_25", "I7037_26", "I7037_27", "I7037_28", "I7037_29", "I7037_30", "I7037_31", "I7037_32", "I7037_33", "I7037_34", "I7037_35", "I7037_36", "I7037_37", "I7037_38", "I7037_39", "I7037_40", "I7037_41", "I7037_42", "I7037_43", "I7037_44", "I7037_45", "I7037_46", "I7037_47", "I7037_48", "I7037_49", "I7037_50", "I7037_51", "I7037_52", "I7037_53", "I7037_54", "I7037_55", "I7037_56", "I7037_57", "I7037_58", "I7037_59", "I7037_60", "I7037_61", "I7037_62", "I7037_63", "I7037_64", "I7037_65", "I7037_66", "I7037_67", "I7037_68", "I7037_69", "I7037_70", "I7037_71"],
        "dims": ["Country", "StateName", "StateCode", "Year"],
    },
    {
        "source_code": 7035,
        "name": "RHS_Districtwise",
        "indicators": ["I7035_4", "I7035_5", "I7035_6", "I7035_7", "I7035_8", "I7035_9", "I7035_10"],
        "dims": ["Country", "StateName", "StateCode", "DistrictName", "DistrictCode", "Year"],
    },
    # ── NSS Round 75 ───────────────────────────────────────────────────────
    {
        "source_code": 7292,
        "name": "NSS75_ReasonNotUsingGovtHospital",
        "indicators": ["I7292_4", "I7292_5", "I7292_6", "I7292_7", "I7292_8", "I7292_9", "I7292_10", "I7292_11", "I7292_12", "I7292_13", "I7292_14"],
        "dims": ["Country", "StateName", "StateCode", "Year", "TRU", "D7292_3"],
    },
    {
        "source_code": 7298,
        "name": "NSS75_OOP_Expenditure",
        "indicators": ["I7298_5", "I7298_6", "I7298_7"],
        "dims": ["Country", "StateName", "StateCode", "Year", "TRU", "D7298_3", "D7298_4"],
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