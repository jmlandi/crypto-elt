import duckdb
from src.utils.logger import Logger

DB_PATH = "./data/warehouse.raw"  # keep your filename, or change to .duckdb

class BaseTransformer:
    @staticmethod
    def run(con: duckdb.DuckDBPyConnection, ref: str, sql: str):
        Logger.info(f"Executing transform step: {ref}...")
        con.execute(sql)

class MarketDataTransformer(BaseTransformer):

    @staticmethod
    def execute(database: str = DB_PATH):
        Logger.info("Starting data transformation process...")
        try:
            with duckdb.connect(database) as con:
                con.execute("BEGIN")
                MarketDataTransformer._remove_duplicates_from_main(con)
                MarketDataTransformer._create_normalized_coins_table(con)
                MarketDataTransformer._create_normalized_tags_table(con)
                MarketDataTransformer._create_normalized_platform_details_table(con)
                MarketDataTransformer._create_normalized_quotes_table(con)
                MarketDataTransformer._create_analytics_current_prices_table(con)
                con.execute("COMMIT")
            Logger.info("Data transformation process completed successfully.")
        except Exception as e:
            Logger.error(f"Transform failed: {e}")
            raise

    @staticmethod
    def _remove_duplicates_from_main(con):
        MarketDataTransformer.run(
          con,
          "dedupe main.market_data",
          """
            CREATE OR REPLACE TABLE main.market_data AS
            SELECT DISTINCT * FROM main.market_data;
          """)

    @staticmethod
    def _create_normalized_coins_table(con):
        MarketDataTransformer.run(
          con,
          "create or update normalized.coins",
          """
            CREATE OR REPLACE TABLE normalized.coins AS
              SELECT
                id, name, symbol, slug, cmc_rank, num_market_pairs,
                circulating_supply, total_supply, max_supply,
                date_added, last_updated
              FROM main.market_data;
          """)

    @staticmethod
    def _create_normalized_tags_table(con):
        # tags assumed LIST<TEXT>
        MarketDataTransformer.run(
          con,
          "normalized.tags",
          """
            CREATE OR REPLACE TABLE normalized.tags AS
            SELECT
              md.id,
              UNNEST(md.tags) AS tag
            FROM main.market_data md;
          """)

    @staticmethod
    def _create_normalized_platform_details_table(con):
        MarketDataTransformer.run(
          con,
          "normalized.platform_details",
          """
            CREATE OR REPLACE TABLE normalized.platform_details AS
            SELECT
              md.id,
              md.platform.id            AS platform_id,
              md.platform.name          AS platform_name,
              md.platform.symbol        AS platform_symbol,
              md.platform.slug          AS platform_slug,
              md.platform.token_address AS platform_token_address
            FROM main.market_data md
            WHERE md.platform IS NOT NULL;
          """)

    @staticmethod
    def _create_normalized_quotes_table(con):
        MarketDataTransformer.run(con, "normalized.quotes", """
            CREATE OR REPLACE TABLE normalized.quotes AS
            
            SELECT
              id, 'USD' AS currency, UNNEST(quote['USD'])
            FROM warehouse.main.market_data md
            WHERE quote['USD'] IS NOT NULL
                        
              UNION ALL
                                              
            SELECT
              id, 'EUR' AS currency, UNNEST(quote['EUR'])
            FROM warehouse.main.market_data md
            WHERE quote['EUR'] IS NOT NULL
                                              
              UNION ALL
                                              
            SELECT
              id, 'BRL' AS currency, UNNEST(quote['BRL'])
            FROM warehouse.main.market_data md
            WHERE quote['BRL'] IS NOT NULL;
          """)

    @staticmethod
    def _create_analytics_current_prices_table(con):
        MarketDataTransformer.run(
          con,
          "analytics.current_prices",
          """
            CREATE OR REPLACE TABLE analytics.current_prices AS
            WITH last_quote_values AS (
              SELECT *
              FROM (
                SELECT q.*,
                       ROW_NUMBER() OVER (PARTITION BY q.id, q.currency ORDER BY q.last_updated DESC) AS rn
                FROM normalized.quotes q
              )
              WHERE rn = 1
            )
            SELECT
              c.id,
              c.name,
              c.symbol,
              q.currency,
              q.price,
              ROW_NUMBER() OVER (PARTITION BY q.currency ORDER BY q.price DESC) AS price_rank,
              q.last_updated
            FROM normalized.coins c
            LEFT JOIN last_quote_values q
              USING (id)
            ORDER BY q.last_updated DESC;
          """)
