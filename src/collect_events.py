from dotenv import load_dotenv
from pathlib import Path
import requests
import pandas as pd
import os
from datetime import datetime

load_dotenv()

API_KEY = os.environ.get("TICKETMASTER_API_KEY")
url = "https://app.ticketmaster.com/discovery/v2/events.json"
postalCode = os.environ.get("POSTAL_CODE")

# API payload
payload = {
    "postalCode": postalCode,
    "apikey": API_KEY
    }

response = requests.get(url, params=payload)

# Checks if API is up and running and stores data into CSV
if response.status_code == 200:
    data = response.json()

    events_list = []
    current_time = datetime.now()


    for event in data['_embedded']['events']:
        event_name = event['name']  
        event_id = event['id']
        event_date = event['dates']['start']['localDate']
        event_time = event['dates']['start']['localTime']
        venue_name = event["_embedded"]["venues"][0]["name"]
        venue_lat = event["_embedded"]["venues"][0]["location"]["latitude"]
        venue_lon = event["_embedded"]["venues"][0]["location"]["longitude"]
        genre = event["classifications"][0]["genre"]["name"]

        row = {
            "Event_name": event_name,
            "Event_ID": event_id,
            "Event_date": event_date,
            "Event_time": event_time,
            "Venue_name": venue_name,
            "Venue_lat": venue_lat,
            "Venue_lon": venue_lon,
            "Genre": genre,
            "Collection_time": current_time
        }

        events_list.append(row)

    # Builds path to data folder
    script_dir = Path(__file__).parent
    data_dir = script_dir.parent / "data"
    os.makedirs(data_dir, exist_ok=True)

    csv_path = data_dir / "events_data.csv"

    # Creates pipeline
    df = pd.DataFrame(events_list)
    if os.path.exists(csv_path):
        df.to_csv(csv_path, mode='a', header=False, index=False)
    else:
        df.to_csv(csv_path, mode='w', header=True, index=False)
        
    print(f"Successfully saved {len(events_list)} events entries to CSV")

else:
    print(f"Error: {response.status_code}")
