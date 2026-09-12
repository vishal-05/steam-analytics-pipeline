import pandas as pd
import json
import glob
import os
from datetime import datetime, timezone


def load_json_files(bronze_dir: str, pattern: str) -> list[dict]:
    """Read and flatten all matching JSON files in a bronze partition."""
    records = []
    for filepath in glob.glob(os.path.join(bronze_dir, pattern)):
        with open(filepath, "r") as f:
            data = json.load(f)
            records.extend(data)
    return records


def transform_player_counts(bronze_dir: str) -> tuple[pd.DataFrame, dict]:
    raw = load_json_files(bronze_dir, "player_counts_*.json")
    raw_count = len(raw)

    df = pd.DataFrame(raw)
    reject_log = {"stage": "player_counts", "raw_records": raw_count}

    # --- Null/type handling ---
    before = len(df)
    df = df.dropna(subset=["appid", "player_count"])
    reject_log["dropped_nulls"] = before - len(df)

    df["appid"] = df["appid"].astype(int)
    df["player_count"] = pd.to_numeric(df["player_count"], errors="coerce")
    before = len(df)
    df = df.dropna(subset=["player_count"])  # catch anything coerce turned to NaN
    reject_log["dropped_bad_types"] = before - len(df)
    df["player_count"] = df["player_count"].astype(int)

    df["pulled_at"] = pd.to_datetime(df["pulled_at"], errors="coerce", utc=True)
    before = len(df)
    df = df.dropna(subset=["pulled_at"])
    reject_log["dropped_bad_timestamps"] = before - len(df)

    # --- Dedupe: keep latest pull per appid (multiple test runs collapse to one) ---
    before = len(df)
    df = df.sort_values("pulled_at").drop_duplicates(subset=["appid"], keep="last")
    reject_log["deduped_rows"] = before - len(df)

    reject_log["final_records"] = len(df)
    return df, reject_log


def transform_store_details(bronze_dir: str) -> tuple[pd.DataFrame, dict]:
    raw = load_json_files(bronze_dir, "store_details_*.json")
    raw_count = len(raw)

    df = pd.DataFrame(raw)
    reject_log = {"stage": "store_details", "raw_records": raw_count}

    before = len(df)
    df = df.dropna(subset=["appid", "game_name"])
    reject_log["dropped_nulls"] = before - len(df)

    df["appid"] = df["appid"].astype(int)
    df["price_current"] = pd.to_numeric(df["price_current"], errors="coerce").fillna(0.0)
    df["price_original"] = pd.to_numeric(df["price_original"], errors="coerce").fillna(0.0)
    df["discount_pct"] = pd.to_numeric(df["discount_pct"], errors="coerce").fillna(0)
    df["is_free"] = df["is_free"].astype(bool)

    # --- Data-quality flag: paid game but price came back as 0 (Steam API gap, not truly free) ---
    df["price_data_missing"] = (~df["is_free"]) & (df["price_current"] == 0.0)

    # --- keep reference fields needed downstream for dim_game / dim_publisher ---
    df["genres"] = df["genres"].apply(lambda g: g if isinstance(g, list) else [])
    df["developers"] = df["developers"].apply(lambda d: d if isinstance(d, list) else [])
    df["publishers"] = df["publishers"].apply(lambda p: p if isinstance(p, list) else [])
    df["release_date"] = df.get("release_date", pd.Series([None] * len(df)))

    df["pulled_at"] = pd.to_datetime(df["pulled_at"], errors="coerce", utc=True)
    before = len(df)
    df = df.dropna(subset=["pulled_at"])
    reject_log["dropped_bad_timestamps"] = before - len(df)

    before = len(df)
    df = df.sort_values("pulled_at").drop_duplicates(subset=["appid"], keep="last")
    reject_log["deduped_rows"] = before - len(df)

    reject_log["final_records"] = len(df)
    reject_log["price_data_missing_count"] = int(df["price_data_missing"].sum())
    return df, reject_log


def run_silver_transform(ds: str,
                          bronze_root: str = "/opt/airflow/bronze",
                          silver_root: str = "/opt/airflow/silver"):
    bronze_dir = os.path.join(bronze_root, f"dt={ds}")

    pc_df, pc_log = transform_player_counts(bronze_dir)
    sd_df, sd_log = transform_store_details(bronze_dir)

    pc_out_dir = os.path.join(silver_root, "player_counts", f"dt={ds}")
    sd_out_dir = os.path.join(silver_root, "store_details", f"dt={ds}")
    os.makedirs(pc_out_dir, exist_ok=True)
    os.makedirs(sd_out_dir, exist_ok=True)

    pc_df.to_parquet(os.path.join(pc_out_dir, "player_counts.parquet"), index=False)
    sd_df.to_parquet(os.path.join(sd_out_dir, "store_details.parquet"), index=False)

    # --- Reject/quality log — this is your resume KPI ---
    log_path = os.path.join(silver_root, "_quality_logs", f"dt={ds}.json")
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    with open(log_path, "w") as f:
        json.dump({
            "run_at": datetime.now(timezone.utc).isoformat(),
            "date_partition": ds,
            "player_counts": pc_log,
            "store_details": sd_log,
        }, f, indent=2)

    print(f"Silver transform complete for dt={ds}")
    print(f"  player_counts: {pc_log}")
    print(f"  store_details: {sd_log}")

    return pc_log, sd_log


if __name__ == "__main__":
    # Standalone test run against today's partition
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    run_silver_transform(today)