import requests
from src.utils.logger import Logger 
import json
from dotenv import load_dotenv
import os

load_dotenv()
url = os.getenv("MARKETCAP_API_URL")
api_key = os.getenv("MARKETCAP_API_KEY")
  
class MarketDataExtractor:
  @staticmethod
  def execute():
    if not url or not api_key:
      raise ValueError("Environment variables for API URL and key are not set.")
    Logger.info("Fetching market data from CoinMarketCap API...")
    
    headers = {
      'Accepts': 'application/json',
      'X-CMC_PRO_API_KEY': api_key
    }

    convert_list = ['USD', 'EUR', 'BRL']

    params = {
      'start': '1',
      'limit': '50',
      'convert': convert_list[0]
    }
    
    session = requests.Session()
    session.headers.update(headers)

    try:
      json_data = []
      for i in range(len(convert_list)):
        params['convert'] = convert_list[i]
        response = session.get(url + '/v1/cryptocurrency/listings/latest', params=params)
        Logger.debug(f"API Response Status Code: {response.status_code}")
        response.raise_for_status()
        json_data += json.loads(response.text)['data']

      Logger.info("Market data fetched successfully.")
      return json_data
    except (
      requests.exceptions.ConnectionError,
      requests.exceptions.Timeout,
      requests.exceptions.TooManyRedirects
    ) as e:
      Logger.error(f"An error ocurred during data fetching for Market data: {e}")
      raise e