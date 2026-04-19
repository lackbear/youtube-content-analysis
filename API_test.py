from googleapiclient.discovery import build
from dotenv import load_dotenv
import os

load_dotenv()

API_KEY = os.getenv("YOUTUBE_API_KEY")


youtube = build("youtube", "v3", developerKey=API_KEY)

request = youtube.search().list(
    q="data analytics for beginners",
    part="snippet",
    type="video",
    maxResults=5
)

response = request.execute()

for item in response["items"]:
    print(item["snippet"]["title"])