import os
import duckdb
from src.utils.logger import Logger
from src.scripts.pipeline import MarketDataPipeline


class Initializer:

  @staticmethod
  def setup(app):
    Initializer.__initialize_directories()
    Initializer.__initialize_databases()
    Initializer.__initialize_flask(app)
    Logger.info("Initialization completed successfully.")

  def __initialize_directories():
    try:
      os.makedirs('./data/raw', exist_ok=True)
      os.makedirs('./data/analytics', exist_ok=True)
      Logger.info("Data directories initialized successfully.")
    except Exception as e:
      Logger.error(f"Error initializing data directories: {str(e)}")
      raise e

  def __initialize_databases():
    try:
      with duckdb.connect('./data/warehouse.raw') as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS main;")
        con.execute("CREATE SCHEMA IF NOT EXISTS normalized;")
        con.execute("CREATE SCHEMA IF NOT EXISTS analytics;")
      Logger.info("DuckDB databases initialized successfully.")
      MarketDataPipeline.execute()
      Logger.info("Initial data load completed successfully.")
    except Exception as e:
      Logger.error(f"Error initializing DuckDB databases: {str(e)}")
      raise e
  
  def __initialize_flask(app):
    try:
      app.run(host='0.0.0.0', port=3000)
      Logger.info("Flask app initialized successfully.")
    except Exception as e:
      Logger.error(f"Error initializing Flask app: {str(e)}")
      raise e
