import requests
import json
from datetime import datetime, timezone
import os

# A small starter list — mix of AAA live-service and mid-tier titles
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

def fetch_player_count(appid: int) -> dict:
    url = "https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/"
    response = requests.get(url, params={"appid": appid}, timeout=10)
    response.raise_for_status()
    data = response.json()
    return {
        "appid": appid,
        "game_name": TRACKED_APPS[appid],
        "player_count": data["response"]["player_count"],
        "pulled_at": datetime.now(timezone.utc).isoformat(),
    }

def fetch_all_and_save(output_dir: str = "/opt/airflow/scripts/output"):
    os.makedirs(output_dir, exist_ok=True)
    results = [fetch_player_count(appid) for appid in TRACKED_APPS]

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filepath = os.path.join(output_dir, f"player_counts_{timestamp}.json")

    with open(filepath, "w") as f:
        json.dump(results, f, indent=2)

    print(f"Saved {len(results)} records to {filepath}")
    return filepath

if __name__ == "__main__":
    fetch_all_and_save()