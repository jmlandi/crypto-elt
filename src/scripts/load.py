import duckdb
import pandas as pd
from src.utils.logger import Logger


class DataLoader:
  @staticmethod
  def execute(data, name):
    DataLoader.__save_to_csv(data, name)
    DataLoader.__save_to_duckdb(data, name)

  def __save_to_csv(data, filename):
    try:
      if not data:
        Logger.warning("No data to save to CSV.")
        return
      Logger.info(f"Saving data to {filename}...")
      df = pd.DataFrame(data)
      df.to_csv(f"./data/raw/{filename}.csv", index=False)
      Logger.info("Data saved to CSV successfully.")
    except Exception as e:
      Logger.error(f"Error saving data to CSV: {str(e)}")
      raise e

  def __save_to_duckdb(data, tablename):
    try:
      if not data:
        Logger.warning("No data to save to DuckDB.")
        return
      Logger.info(f"Saving data to DuckDB table '{tablename}'...")
      df = pd.DataFrame(data)
      with duckdb.connect('./data/warehouse.raw') as con:
        con.register('df', df)
        con.sql(f"CREATE OR REPLACE TABLE {tablename} AS SELECT * FROM df")
      Logger.info(f"Data saved to DuckDB table '{tablename}' successfully.")
      # Logger.debug(f"Data saved:\n{df.head()}")
    except Exception as e:
      Logger.error(f"Error saving data to DuckDB: {str(e)}")
      raise e