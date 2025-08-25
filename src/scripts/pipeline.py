from src.scripts.extract import MarketDataExtractor
from src.scripts.load import DataLoader
from src.utils.logger import Logger

class MarketDataPipeline:
  filename = 'market_data'
  
  @staticmethod
  def execute():
    try:
      data = MarketDataExtractor.execute()
      DataLoader.execute(data, MarketDataPipeline.filename)
    except Exception as e:
      Logger.error(f"An error occurred during extraction and loading: {e}")