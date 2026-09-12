import requests
import json
import time
from datetime import datetime, timezone
import os

TRACKED_APPS = {
    730: "Counter-Strike 2",
    570: "Dota 2",
    578080: "PUBG: BATTLEGROUNDS",
    1172470: "Apex Legends",
    271590: "Grand Theft Auto V",
    1091500: "Cyberpunk 2077",
    1245620: "Elden Ring",
    1174180: "Red Dead Redemption 2",
    413150: "Stardew Valley",
    892970: "Valheim",
    1938090: "Call of Duty",
    1085660: "Destiny 2",
    252490: "Rust",
    582010: "Monster Hunter: World",
    1203220: "NARAKA: BLADEPOINT",
    1063730: "New World",
    1517290: "Battlefield 2042",
    381210: "Dead by Daylight",
    1811260: "EA SPORTS FC 24",
    1290000: "Forza Horizon 5",
    1097150: "Fall Guys",
    1237970: "Titanfall 2",
    632360: "Risk of Rain 2",
    1449850: "Yu-Gi-Oh! Master Duel",
    1966720: "Lethal Company",
    2050650: "Resident Evil 4",
    1145360: "Hades",
    1030300: "Hollow Knight: Silksong",
    346110: "ARK: Survival Evolved",
    440900: "Conan Exiles",
}

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

def fetch_store_details(appid: int) -> dict | None:
    url = "https://store.steampowered.com/api/appdetails"
    params = {"appids": appid, "cc": "us"}  # no filters param — it breaks the response
    response = requests.get(url, params=params, headers=HEADERS, timeout=10)
    response.raise_for_status()
    data = response.json()

    entry = data.get(str(appid), {})
    if not entry.get("success"):
        print(f"WARNING: appid {appid} ({TRACKED_APPS.get(appid)}) returned success=False, skipping")
        return None

    details = entry.get("data")
    if not isinstance(details, dict):
        print(f"WARNING: appid {appid} ({TRACKED_APPS.get(appid)}) returned empty/malformed data, skipping")
        return None

    is_free = details.get("is_free", False)
    price = details.get("price_overview")  # missing entirely for free games

    if price:
        price_current = price.get("final", 0) / 100
        price_original = price.get("initial", 0) / 100
        discount_pct = price.get("discount_percent", 0)
        currency = price.get("currency", "USD")
    else:
        price_current = 0.0
        price_original = 0.0
        discount_pct = 0
        currency = "USD"

    return {
        "appid": appid,
        "game_name": TRACKED_APPS[appid],
        "is_free": is_free,
        "price_current": price_current,
        "price_original": price_original,
        "discount_pct": discount_pct,
        "currency": currency,
        # bonus fields — feed your weekly dim_game/dim_publisher pull later
        "genres": [g["description"] for g in details.get("genres", [])],
        "developers": details.get("developers", []),
        "publishers": details.get("publishers", []),
        "release_date": details.get("release_date", {}).get("date"),
        "pulled_at": datetime.now(timezone.utc).isoformat(),
    }

def fetch_all_and_save(output_dir: str = "/opt/airflow/scripts/output"):
    os.makedirs(output_dir, exist_ok=True)
    results = []

    for appid in TRACKED_APPS:
        record = fetch_store_details(appid)
        if record:
            results.append(record)
        time.sleep(1.5)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filepath = os.path.join(output_dir, f"store_details_{timestamp}.json")

    with open(filepath, "w") as f:
        json.dump(results, f, indent=2)

    print(f"Saved {len(results)} records to {filepath}")
    return filepath

if __name__ == "__main__":
    fetch_all_and_save()