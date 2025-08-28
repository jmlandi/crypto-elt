from src.scripts.extract import MarketDataExtractor
from src.scripts.load import DataLoader
from src.scripts.transform import MarketDataTransformer
from src.utils.logger import Logger

class MarketDataPipeline:
  filename = 'market_data'
  
  @staticmethod
  def execute():
    try:
      Logger.info("Starting ETL pipeline...")
      data = MarketDataExtractor.execute()
      DataLoader.execute(data, MarketDataPipeline.filename)
      MarketDataTransformer.execute()
      Logger.info("ETL pipeline executed successfully.")
    except Exception as e:
      Logger.error(f"An error occurred during extraction and loading: {e}")
      raise e