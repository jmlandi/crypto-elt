from scripts.extract import fetch_market_data
from scripts.load import save_to_csv, save_to_duckdb
from utils.logger import Logger

def extract_and_load_market_data():
  try:
    data = fetch_market_data()
    name = 'market_data'
    save_to_csv(data, f"{name}.csv")
    save_to_duckdb(data, name)
  except Exception as e:
    Logger.error(f"An error occurred during extraction and loading: {e}")