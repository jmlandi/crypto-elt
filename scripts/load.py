import duckdb
import pandas as pd
from utils.logger import Logger

def save_to_csv(data, filename='market_data.csv'):
  if not data:
    Logger.warning("No data to save to CSV.")
    return
  Logger.info(f"Saving data to {filename}...")
  df = pd.DataFrame(data)
  df.to_csv(f"./data/raw/{filename}", index=False)
  Logger.info("Data saved to CSV successfully.")


def save_to_duckdb(data, tablename='market_data'):
  if not data:
    Logger.warning("No data to save to DuckDB.")
    return
  Logger.info(f"Saving data to DuckDB table '{tablename}'...")
  df = pd.DataFrame(data)
  with duckdb.connect('warehouse.duckdb') as con:
    con.register('df', df)
    con.sql(f"CREATE OR REPLACE TABLE {tablename} AS SELECT * FROM df")
  Logger.info(f"Data saved to DuckDB table '{tablename}' successfully.")
  Logger.debug(f"Data saved:\n{df.head()}")