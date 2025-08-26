from src.utils.logger import Logger
from src.scripts.pipeline import MarketDataPipeline
from flask import jsonify
import duckdb

class Controller:
  @staticmethod
  def root():
    Logger.info("Root endpoint accessed.")
    return "Welcome to the Cryptocurrency Market Data Pipeline! App is running.", 200
  
  @staticmethod
  def refresh_data():
    try:
      Logger.info("Starting data refresh...")
      MarketDataPipeline.execute()
      Logger.info("Data refresh completed successfully.")
      return "Data refresh completed successfully.", 200
    except Exception as e:
      Logger.error(f"Error during data refresh: {str(e)}")
      return f"Error during data refresh: {str(e)}", 500
    
  @staticmethod
  def query_data(query, dataset='raw'):
    try:
      Logger.info(f"Executing query: {query}")
      with duckdb.connect(f"warehouse.{dataset}") as con:
        result = con.execute(query).fetchall()
      Logger.info("Query executed successfully.")
      return jsonify(result), 200
    except Exception as e:
      Logger.error(f"Error during query execution: {str(e)}")
      return f"Error during query execution: {str(e)}", 500