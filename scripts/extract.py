import requests
from utils.logger import Logger 
import json
from dotenv import load_dotenv
import os

load_dotenv()
url = os.getenv("MARKETCAP_API_URL")
api_key = os.getenv("MARKETCAP_API_KEY")
  
def fetch_market_data():
  if not url or not api_key:
    raise ValueError("Environment variables for API URL and key are not set.")
  Logger.info("Fetching market data from CoinMarketCap API...")
  
  headers = {
    'Accepts': 'application/json',
    'X-CMC_PRO_API_KEY': api_key
  }
  params = {
    'start': '1',
    'limit': '3',
    'convert': 'USD'
  }
  
  session = requests.Session()
  session.headers.update(headers)

  try:
    response = session.get(url + '/v1/cryptocurrency/listings/latest', params=params)
    json_data = json.loads(response.text)['data']
    Logger.info("Market data fetched successfully.")
    return json_data
  except (
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
    requests.exceptions.TooManyRedirects
  ) as e:
    Logger.error(f"An error ocurred during data fetching for Market data: {e}")
    raise e