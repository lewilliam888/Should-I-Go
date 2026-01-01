from dotenv import load_dotenv
from pathlib import Path
import requests
import pandas as pd
import os
from datetime import datetime

load_dotenv()

API_KEY = os.environ.get("OPENWEATHER_API_KEY")
url = "https://api.openweathermap.org/data/2.5/forecast"
location_name = "Downtown Cary Park"

# Venues data
venues = [
    {
        "name": "Bucked Up Fitness",
        "type": "gym",
        "lat": 35.802734668474415,
        "lon": -78.81434264989247
    },
    {
        "name": "Chipotle",
        "type": "restaurant",
        "lat": 35.80714078948644,
        "lon": -78.81518206126412
    },
    {
        "name": "Target",
        "type": "shopping",
        "lat": 35.8052091465852,
        "lon": -78.81547173983324
    },
    {
        "name": "Raleigh State Farmers Market",
        "type": "market",
        "lat": 35.763155088843476,
        "lon": -78.6624502189379
    },
    {
        "name": "Talley Student Union",
        "type": "student_union",
        "lat": 35.784029725454744,
        "lon": -78.67086207290619
    },
    {
        "name": "Sweet Talk Cafe",
        "type": "coffee_shop",
        "lat": 35.74590293431373,
        "lon": -78.88435401523617
    },
    {
        "name": "Harris Teeter",
        "type": "grocery_store",
        "lat": 35.80682615901181,
        "lon": -78.78030468747359
    },
    {
        "name": "Fenton",
        "type": "shopping_center",
        "lat": 35.78153503676146,
        "lon": -78.75614398152707
    },
    {
        "name": "Downtown Cary Park",
        "type": "park",
        "lat": 35.78400237834643,
        "lon": -78.7809995798104
    },
    {
        "name": "James B Hunt Library",
        "type": "library",
        "lat": 35.769464978870666,
        "lon": -78.67645262872736
    }
]

# API payload
payload = {
        "lat": 35.78400237834643,
        "lon": -78.7809995798104,
        "appid": API_KEY,
        "units": "imperial"
    }

response = requests.get(url, params=payload)

# Checks if API is up and running and stores data into CSV
if response.status_code == 200:
    data = response.json()

    #print(response.status_code)
    #print(data)

    weather_list = []
    current_time = datetime.now()

    for entry in data['list']:
        dt_txt = entry['dt_txt']  
        temp = entry['main']['temp']
        humidity = entry['main']['humidity']
        weather = entry['weather'][0]['description']
        pop = entry['pop'] * 100

        row = {
            "Date_Time": dt_txt,
            "Temp": temp,
            "Humidity": humidity,
            "Weather": weather,
            "Precip_Prob": pop,
            "Current_time": current_time
        }

        weather_list.append(row)

    #print(weather_list)
    #print(f"{dt_txt} | {temp}°F | {weather} | {pop}%")

    # Builds path to data folder
    script_dir = Path(__file__).parent
    data_dir = script_dir.parent / "data"
    os.makedirs(data_dir, exist_ok=True)

    csv_path = data_dir / "weather_data.csv"

    # Creates pipeline
    df = pd.DataFrame(weather_list)
    if os.path.exists("../data/weather_data.csv"):
        df.to_csv(csv_path, mode='a', header=False, index=False)
    else:
        df.to_csv(csv_path, mode='w', header=True, index=False)
        
    print(f"Successfully saved {len(weather_list)} weather entries to CSV")

else:
    print(f"Error: {response.status_code}")
