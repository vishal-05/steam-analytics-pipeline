import pandas as pd
import sqlite3
import os
from datetime import datetime, timezone

SILVER_ROOT = "/opt/airflow/silver"
GOLD_ROOT = "/opt/airflow/gold"
DB_PATH = "/opt/airflow/warehouse/steam_star_schema.db"


def _to_list(x):
    """Normalize genres/publishers/developers from parquet round-trip
    (numpy arrays) back to plain Python lists."""
    if x is None:
        return []
    if isinstance(x, (list, tuple)):
        return list(x)
    if hasattr(x, "tolist"):  # numpy array
        return x.tolist()
    return []


# ---------------------------------------------------------------------------
# dim_date
# ---------------------------------------------------------------------------
def build_dim_date(start: str, end: str) -> pd.DataFrame:
    dates = pd.date_range(start=start, end=end, freq="D")
    df = pd.DataFrame({"full_date": dates})
    df["date_key"] = df["full_date"].dt.strftime("%Y%m%d").astype(int)
    df["day_of_week"] = df["full_date"].dt.dayofweek
    df["day_name"] = df["full_date"].dt.day_name()
    df["month"] = df["full_date"].dt.month
    df["month_name"] = df["full_date"].dt.month_name()
    df["quarter"] = df["full_date"].dt.quarter
    df["year"] = df["full_date"].dt.year
    df["is_weekend"] = df["day_of_week"].isin([5, 6])
    return df[["date_key", "full_date", "day_of_week", "day_name",
               "month", "month_name", "quarter", "year", "is_weekend"]]


# ---------------------------------------------------------------------------
# dim_publisher (Type 2 SCD)
# ---------------------------------------------------------------------------
def build_dim_publisher(store_details_latest: pd.DataFrame, as_of_date: str,
                         existing: pd.DataFrame | None) -> pd.DataFrame:
    exploded = store_details_latest.copy()
    exploded["publishers"] = exploded["publishers"].apply(_to_list)
    exploded = exploded.explode("publishers")
    pub_names = exploded["publishers"].dropna().unique()
    incoming = pd.DataFrame({"publisher_name": pub_names})

    if existing is None or existing.empty:
        incoming["publisher_key"] = range(1, len(incoming) + 1)
        incoming["effective_start_date"] = as_of_date
        incoming["effective_end_date"] = None
        incoming["is_current"] = True
        return incoming

    current = existing[existing["is_current"]]
    new_names = set(incoming["publisher_name"]) - set(current["publisher_name"])

    max_key = existing["publisher_key"].max()
    new_rows = pd.DataFrame({"publisher_name": list(new_names)})
    if not new_rows.empty:
        new_rows["publisher_key"] = range(max_key + 1, max_key + 1 + len(new_rows))
        new_rows["effective_start_date"] = as_of_date
        new_rows["effective_end_date"] = None
        new_rows["is_current"] = True

    return pd.concat([existing, new_rows], ignore_index=True)


# ---------------------------------------------------------------------------
# dim_game (Type 1 — overwrite)
# ---------------------------------------------------------------------------
def build_dim_game(store_details_latest: pd.DataFrame, dim_publisher: pd.DataFrame) -> pd.DataFrame:
    df = store_details_latest.copy()
    df["genres"] = df["genres"].apply(_to_list)
    df["publishers"] = df["publishers"].apply(_to_list)

    df["primary_genre"] = df["genres"].apply(lambda g: g[0] if g else "Unknown")
    df["all_genres"] = df["genres"].apply(lambda g: ", ".join(g) if g else "")
    df["primary_publisher"] = df["publishers"].apply(lambda p: p[0] if p else "Unknown")

    current_pub = dim_publisher[dim_publisher["is_current"]][["publisher_name", "publisher_key"]]
    df = df.merge(current_pub, left_on="primary_publisher", right_on="publisher_name", how="left")

    df["game_key"] = range(1, len(df) + 1)
    df = df.rename(columns={"appid": "app_id", "game_name": "game_name"})

    return df[["game_key", "app_id", "game_name", "primary_genre", "all_genres",
               "publisher_key", "release_date", "is_free"]]


# ---------------------------------------------------------------------------
# fact_game_performance
# ---------------------------------------------------------------------------
def build_fact(daily_snapshot: pd.DataFrame, dim_game: pd.DataFrame, dim_date: pd.DataFrame) -> pd.DataFrame:
    df = daily_snapshot.copy()
    df["date_key"] = pd.to_datetime(df["date"]).dt.strftime("%Y%m%d").astype(int)

    df = df.merge(dim_game[["app_id", "game_key"]], left_on="appid", right_on="app_id", how="left")
    df = df.merge(dim_date[["date_key"]], on="date_key", how="inner")

    df["fact_key"] = range(1, len(df) + 1)
    df["load_timestamp"] = datetime.now(timezone.utc).isoformat()

    return df[["fact_key", "date_key", "game_key", "player_count",
               "price_current", "price_original", "discount_pct",
               "revenue_proxy", "load_timestamp"]]


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
def run_star_schema_build(ds: str):
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)

    sd_path = os.path.join(SILVER_ROOT, "store_details", f"dt={ds}", "store_details.parquet")
    if not os.path.exists(sd_path):
        raise FileNotFoundError(f"No store_details silver data for {ds} — run the weekly branch first")
    store_details = pd.read_parquet(sd_path)

    snap_path = os.path.join(GOLD_ROOT, "daily_snapshot", f"dt={ds}", "daily_snapshot.parquet")
    daily_snapshot = pd.read_parquet(snap_path)

    dim_date = build_dim_date("2026-01-01", "2026-12-31")
    dim_date.to_sql("dim_date", conn, if_exists="replace", index=False)

    try:
        existing_pub = pd.read_sql("SELECT * FROM dim_publisher", conn)
        existing_pub["is_current"] = existing_pub["is_current"].astype(bool)
    except pd.errors.DatabaseError:
        existing_pub = None
    dim_publisher = build_dim_publisher(store_details, ds, existing_pub)
    dim_publisher.to_sql("dim_publisher", conn, if_exists="replace", index=False)

    dim_game = build_dim_game(store_details, dim_publisher)
    dim_game.to_sql("dim_game", conn, if_exists="replace", index=False)

    fact = build_fact(daily_snapshot, dim_game, dim_date)
    fact.to_sql("fact_game_performance", conn, if_exists="append", index=False)

    conn.close()
    print(f"Star schema updated for {ds}")
    print(f"  dim_date: {len(dim_date)} rows")
    print(f"  dim_publisher: {len(dim_publisher)} rows ({dim_publisher['is_current'].sum()} current)")
    print(f"  dim_game: {len(dim_game)} rows")
    print(f"  fact_game_performance: +{len(fact)} rows this run")


if __name__ == "__main__":
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    run_star_schema_build(today)