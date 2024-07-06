# src/data_fetcher.py
import requests
import logging

API_URL = "https://randomuser.me/api/"

def get_data():
    try:
        res = requests.get(API_URL)
        res.raise_for_status()  # Raise an error for bad status codes
        res = res.json()
        res = res['results'][0]
        return res
    except requests.RequestException as e:
        logging.error(f"Error fetching data: {e}")
        return None