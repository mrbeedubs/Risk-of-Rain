import os
import time
import requests
import pandas as pd
import streamlit as st
from geopy.geocoders import Nominatim
from datetime import datetime

# === Constants ===
MM_TO_INCHES = 0.0393701
RESULTS_PER_PAGE = 1000

# === NOAA Token from Secrets or Input ===
st.set_page_config(page_title="Rain Risk Analyzer", layout="centered")
st.title("🌧️ Rain Risk Analyzer (NOAA Historical Data)")

with st.sidebar:
    st.header("🔧 Settings")
    NOAA_API_TOKEN = st.text_input("NOAA API Token", type="password", value=os.getenv("NOAA_API_TOKEN", ""))
    address = st.text_input("Jobsite Address", "123 Main St, Springfield, IL")
    start_year = st.number_input("Start Year", min_value=2000, max_value=datetime.now().year, value=2020)
    end_year = st.number_input("End Year", min_value=2000, max_value=datetime.now().year, value=2024)
    threshold_in = st.number_input("Rain Threshold (inches)", min_value=0.1, max_value=3.0, value=0.5)
    run_button = st.button("Run Analysis")

# === Core Functions ===
def get_coordinates(address):
    geolocator = Nominatim(user_agent="weather_app_streamlit")
    location = geolocator.geocode(address)
    if not location:
        raise ValueError("Could not geocode address.")
    return location.latitude, location.longitude

def find_nearest_station(lat, lon, token):
    url = "https://www.ncdc.noaa.gov/cdo-web/api/v2/stations"
    headers = {"token": token}
    params = {
        "datasetid": "GHCND",
        "latitude": lat,
        "longitude": lon,
        "sortfield": "distance",
        "sortorder": "asc",
        "limit": 1
    }
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    results = response.json().get("results", [])
    return results[0]["id"] if results else None

def fetch_precipitation_data(station_id, token, start_year, end_year):
    url = "https://www.ncdc.noaa.gov/cdo-web/api/v2/data"
    headers = {"token": token}
    all_data = []

    for year in range(start_year, end_year + 1):
        offset = 1
        while True:
            params = {
                "datasetid": "GHCND",
                "stationid": station_id,
                "datatypeid": "PRCP",
                "startdate": f"{year}-01-01",
                "enddate": f"{year}-12-31",
                "limit": RESULTS_PER_PAGE,
                "offset": offset,
                "units": "metric"
            }
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            batch = response.json().get("results", [])
            if not batch:
                break
            all_data.extend(batch)
            offset += RESULTS_PER_PAGE
            time.sleep(0.2)
    return all_data

def process_data(data, threshold):
    df = pd.DataFrame(data)
    if df.empty:
        return None
    df["value"] = df["value"] * MM_TO_INCHES
    df["date"] = pd.to_datetime(df["date"])
    df = df[df["value"] >= threshold]
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month
    monthly_counts = df.groupby(["year", "month"]).size().reset_index(name="days")
    avg_days = monthly_counts.groupby("month")["days"].mean().reset_index()
    month_map = {i: m for i, m in enumerate([
        "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December"
    ], start=1)}
    avg_days["month"] = avg_days["month"].map(month_map)
    return avg_days[["month", "days"]].rename(columns={"days": f"Avg Days ≥ {threshold:.2f} in"})

# === Main Execution ===
if run_button:
    try:
        with st.spinner("🔍 Geocoding address and finding station..."):
            lat, lon = get_coordinates(address)
            station_id = find_nearest_station(lat, lon, NOAA_API_TOKEN)

        with st.spinner(f"📡 Fetching rainfall data from {start_year} to {end_year}..."):
            data = fetch_precipitation_data(station_id, NOAA_API_TOKEN, start_year, end_year)

        with st.spinner("🧮 Calculating averages..."):
            result = process_data(data, threshold_in)
            if result is not None:
                st.success("✅ Analysis complete")
                st.subheader(f"Average Days Per Month with ≥ {threshold_in:.2f} Inches of Rain")
                st.dataframe(result.style.format({f"Avg Days ≥ {threshold_in:.2f} in": "{:.2f}"}), use_container_width=True)

                # Download option
                csv = result.to_csv(index=False).encode("utf-8")
                st.download_button("⬇️ Download CSV", data=csv, file_name=f"rain_days_{start_year}_{end_year}.csv", mime="text/csv")
            else:
                st.warning("No qualifying rain days found in the selected time period.")
    except Exception as e:
        st.error(f"❌ Error: {e}")
