import json
import os
import requests
import time

# Load config.json
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "config", "config.json")
CONFIG_PATH = os.path.abspath(CONFIG_PATH)

with open(CONFIG_PATH, "r") as f:
    config = json.load(f)

API_KEY = config["tfl_api_key"]
BASE_URL = "https://api.tfl.gov.uk"


def make_request(endpoint: str, params: dict = None):
    """
    Generic wrapper to call TfL API with retry & key injection.
    """
    if params is None:
        params = {}
    params["app_key"] = API_KEY

    retries = 3
    for attempt in range(retries):
        try:
            response = requests.get(f"{BASE_URL}{endpoint}", params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"⚠️ Error: {e}, attempt {attempt+1}/{retries}")
            time.sleep(2)
    raise Exception(f"❌ Failed after {retries} attempts: {endpoint}")


# ---- Specific API calls ----
def fetch_line_status():
    return make_request("/Line/Mode/tube/Status")

def fetch_disruptions():
    return make_request("/Line/Mode/tube/Disruption")

def fetch_arrivals():
    return make_request("/Mode/tube/Arrivals")

def fetch_station_status():
    return make_request("/StopPoint/Mode/tube")

def fetch_journey(from_stop="1000267", to_stop="1000269"):
    endpoint = f"/Journey/JourneyResults/{from_stop}/to/{to_stop}"
    return make_request(endpoint)
