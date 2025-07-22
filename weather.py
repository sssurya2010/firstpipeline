import requests
import pandas as pd
from datetime import datetime as dt
from google.cloud import storage

# Step 1: Country capitals with coordinates
capitals = {
    "USA": (38.8977, -77.0365),
    "India": (28.6139, 77.2090),
    "UK": (51.5074, -0.1278),
    "Canada": (45.4215, -75.6972),
    "Australia": (-35.2809, 149.1300),
    "Germany": (52.5200, 13.4050),
    "France": (48.8566, 2.3522),
    "Japan": (35.6895, 139.6917),
    "Brazil": (-15.7939, -47.8828),
    "South Africa": (-25.7479, 28.2293)
}

# Step 2: Fetch temperature from Open-Meteo
def get_temperature(lat, lon):
    url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current_weather=true"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()['current_weather']['temperature']
    return None

# Step 3: Store in list of dicts
data = []
for country, (lat, lon) in capitals.items():
    temp = get_temperature(lat, lon)
    data.append({"Country": country, "Latitude": lat, "Longitude": lon, "Temperature (°C)": temp})

# Step 4: Create Excel file
timestamp = dt.now().strftime("%Y-%m-%d_%H-%M")
print(timestamp)
df = pd.DataFrame(data)
excel_file = f"country_wise_temp_{timestamp}.xlsx"
df.to_excel(excel_file, index=False)

# Step 5: Upload to GCS
def upload_to_gcs(bucket_name, destination_blob_name, source_file_name):
    client = storage.Client()  # Assumes GOOGLE_APPLICATION_CREDENTIALS is set
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print(f"Uploaded {source_file_name} to gs://{bucket_name}/{destination_blob_name}")

# 🔁 Replace with your GCS bucket and file path
upload_to_gcs(
    bucket_name='test1-surya-bucket',
    destination_blob_name='weather_data/{excel_file}'.format(excel_file=excel_file),
    source_file_name=excel_file
)
