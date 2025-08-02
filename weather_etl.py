import requests
import pandas as pd
from datetime import datetime
import urllib.parse
import pyodbc
import time


API_KEY = '33d5d1d0ecc04cbf824ef339656fa204'.strip()
cities = ['New York', 'Los Angeles', 'Chicago', 'Miami', 'Seattle']

weather_data = []
failed_cities = []

conn = pyodbc.connect(
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=localhost;"
    "Database=WeatherDB;"
    "Trusted_Connection=yes;"
)
cursor = conn.cursor()

for city in cities:
    encoded_city = urllib.parse.quote(city)
    url = f"http://api.openweathermap.org/data/2.5/weather?q={encoded_city}&appid={API_KEY}&units=metric"

    response = requests.get(url)
    data = response.json()

    if response.status_code == 200 and 'main' in data:
        weather = {
            'city': city,
            'temperature': data['main']['temp'],
            'humidity': data['main']['humidity'],
            'weather_description': data['weather'][0]['description'],
            'timestamp': datetime.now(),
            'record_date': datetime.now().date()
        }
        weather_data.append(weather)
        print(f"✅ Collected data for {city}")
        log_status = 'Success'
        log_message = 'Data collected'

    else:
        print(f"\n❌ ERROR for {city}")
        print("Status Code:", response.status_code)
        print("Response:", data)
        failed_cities.append((city, data.get('message', 'Unknown error')))
        log_status = 'Failed'
        log_message = data.get('message', 'Unknown error')


    cursor.execute("""
        INSERT INTO WeatherLog (city, status, message)
        VALUES (?, ?, ?)
    """, city, log_status, log_message)

    time.sleep(1.5)


    df = pd.DataFrame(weather_data)
    print(df)

if failed_cities:
    print("\n🔍 Summary of Failed Cities:")
    for city, reason in failed_cities:
        print(f"{city}: {reason}")


for index, row in df.iterrows():
    cursor.execute("""
        INSERT INTO WeatherData (city, temperature, humidity, weather_description, timestamp, record_date)
        VALUES (?, ?, ?, ?, ?, ?)
    """, row['city'], row['temperature'], row['humidity'], row['weather_description'], row['timestamp'], row['record_date'])

conn.commit()
conn.close()
