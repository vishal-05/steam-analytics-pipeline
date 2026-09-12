import pandas as pd
import glob
import os
from datetime import datetime, timedelta, timezone

SILVER_ROOT = "/opt/airflow/silver"
GOLD_ROOT = "/opt/airflow/gold"


def load_silver_history(table_name: str, up_to_ds: str, lookback_days: int = 30) -> pd.DataFrame:
    """Load all available silver partitions for a table, up to `lookback_days` back from up_to_ds."""
    end_date = datetime.strptime(up_to_ds, "%Y-%m-%d")
    frames = []
    for i in range(lookback_days):
        day = (end_date - timedelta(days=i)).strftime("%Y-%m-%d")
        path = os.path.join(SILVER_ROOT, table_name, f"dt={day}", f"{table_name}.parquet")
        if os.path.exists(path):
            df = pd.read_parquet(path)
            df["date"] = day
            frames.append(df)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def build_daily_snapshot(ds: str) -> pd.DataFrame:
    pc = load_silver_history("player_counts", ds, lookback_days=1)
    sd = load_silver_history("store_details", ds, lookback_days=1)

    if pc.empty:
        print(f"No player_count silver data found for {ds}")
        return pd.DataFrame()

    pc = pc[["appid", "game_name", "player_count", "date"]]

    if sd.empty:
        # weekly branch may not have run this day — snapshot still valid, just no pricing
        snapshot = pc.copy()
        for col in ["price_current", "price_original", "discount_pct", "is_free"]:
            snapshot[col] = None
    else:
        sd = sd[["appid", "price_current", "price_original", "discount_pct", "is_free"]]
        snapshot = pc.merge(sd, on="appid", how="left")

    snapshot["revenue_proxy"] = (
        snapshot["price_current"].fillna(0) * snapshot["player_count"]
    )

    return snapshot


def build_trend(ds: str, lookback_days: int = 30) -> pd.DataFrame:
    history = load_silver_history("player_counts", ds, lookback_days=lookback_days)
    disc_history = load_silver_history("store_details", ds, lookback_days=lookback_days)

    if history.empty:
        print(f"No history available to build trend for {ds}")
        return pd.DataFrame()

    history = history.sort_values(["appid", "date"])

    trend_rows = []
    for appid, group in history.groupby("appid"):
        game_name = group["game_name"].iloc[-1]
        days_tracked = group["date"].nunique()
        avg_7d = group.tail(7)["player_count"].mean()

        pct_change_1d = None
        if len(group) >= 2:
            today_val = group.iloc[-1]["player_count"]
            prev_val = group.iloc[-2]["player_count"]
            if prev_val > 0:
                pct_change_1d = round((today_val - prev_val) / prev_val * 100, 2)

        avg_discount_7d = None
        if not disc_history.empty:
            game_disc = disc_history[disc_history["appid"] == appid].sort_values("date")
            if not game_disc.empty:
                avg_discount_7d = game_disc.tail(7)["discount_pct"].mean()

        trend_rows.append({
            "appid": appid,
            "game_name": game_name,
            "avg_player_count_7d": round(avg_7d, 1),
            "player_count_pct_change_1d": pct_change_1d,
            "avg_discount_pct_7d": avg_discount_7d,
            "days_tracked": days_tracked,
        })

    return pd.DataFrame(trend_rows)


def run_gold_build(ds: str):
    snapshot = build_daily_snapshot(ds)
    trend = build_trend(ds)

    snapshot_dir = os.path.join(GOLD_ROOT, "daily_snapshot", f"dt={ds}")
    trend_dir = os.path.join(GOLD_ROOT, "trend", f"dt={ds}")
    os.makedirs(snapshot_dir, exist_ok=True)
    os.makedirs(trend_dir, exist_ok=True)

    if not snapshot.empty:
        snapshot.to_parquet(os.path.join(snapshot_dir, "daily_snapshot.parquet"), index=False)
        print(f"Gold daily_snapshot: {len(snapshot)} rows written for {ds}")
    if not trend.empty:
        trend.to_parquet(os.path.join(trend_dir, "trend.parquet"), index=False)
        print(f"Gold trend: {len(trend)} rows written for {ds}")

    return snapshot, trend


if __name__ == "__main__":
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    run_gold_build(today)