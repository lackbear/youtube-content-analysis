import googleapiclient.discovery
from dotenv import load_dotenv
import os
import csv

load_dotenv()

API_KEY = os.getenv("YOUTUBE_API_KEY")

youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=API_KEY)

handles = [
    ("Siim Land",           "SiimLand"),
    ("Physionic",           "Physionic"),
    ("Brad Stanfield",      "DrBradStanfield"),
    ("James Bruton",        "jamesbruton"),
    ("Breaking Taps",       "BreakingTaps"),
    ("Ben Felix",           "BenFelixCSI"),
    ("The Swedish Investor","TheSwedishInvestor"),
    ("Shashank Kalanithi",  "ShashankKalanithi"),
    ("Data Vidhya",         "DataVidhya"),
    ("Code With Yu",        "CodeWithYu"),
    ("Electronoobs",        "Electronoobs"),
    ("Nikodem Bartnik",     "nikodembartnik"),
]

results = []

for name, handle in handles:
    try:
        res = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            forHandle=handle
        ).execute()

        if res["items"]:
            ch = res["items"][0]
            results.append({
                "name":        name,
                "handle":      handle,
                "channel_id":  ch["id"],
                "playlist_id": ch["contentDetails"]["relatedPlaylists"]["uploads"],
                "subscribers": ch["statistics"].get("subscriberCount", "hidden"),
                "total_views": ch["statistics"].get("viewCount", "N/A"),
            })
            print(f"✓ {name}: {ch['id']}")
        else:
            print(f"✗ {name}: not found — check handle")

    except Exception as e:
        print(f"✗ {name}: error — {e}")

# Save to CSV for your tracker
with open("competitors.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=results[0].keys())
    writer.writeheader()
    writer.writerows(results)

print(f"\nSaved {len(results)} channels to competitors.csv")