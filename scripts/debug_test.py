import requests

resp = requests.get(
    "https://store.steampowered.com/api/appdetails",
    params={"appids": 730, "cc": "us"},
    headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
    timeout=10,
)
print(resp.status_code)
print(resp.json())