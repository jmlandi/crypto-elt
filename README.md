# 🦆 Crypto ELT → DuckDB (Flask + Docker)

Ingest and transform cryptocurrency market data from **CoinMarketCap** into **DuckDB**, with CSV and Parquet exports.
<img width="100%" alt="funny duck gif" src="https://media4.giphy.com/media/v1.Y2lkPTc5MGI3NjExZHB3dHRhcWM0aDc5cWIwNGgxYnBhcnI1cXRxd3lhcDN4azQwbjloaSZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/VeyCyriTsxn3i/giphy.gif" />



## ⏲️ Quick Start

### Prerequisites
- Docker + Docker Compose
- A CoinMarketCap API key (free) — [get one here](https://coinmarketcap.com/api/pricing/)

### Steps
1. **Clone** this repository.
2. **Create your env file**:
   ```
   cp .env.example .env
   ```
   Open `.env` and set your CoinMarketCap credentials.
3. **Run with Docker Compose**:
   ```
   docker compose up -d
   ```
   > If your Docker uses the old plugin, this also works: `docker-compose up -d`

That’s it — the service starts, pulls market data, writes to DuckDB, and performs the transformation steps.

---

## ⚡️ Endpoints

`/data/refresh` -> Update warehouse data.
`data/query` -> Query into the warehouse. Send a JSON body with "query" as key value and the query as the key value.

---

## 🔎 About

This app performs an **ELT** pipeline (**Extract → Load → Transform**) using data from **CoinMarketCap** and persists results to:

- **DuckDB** database (`./data/warehouse.raw`)
- **.csv files** (`./data/...`)
- **.parquet files** (`./data/...`)

**Possible Improvement:** It’s packaged as a **Flask** application so you can add a health endpoint, trigger runs on demand, or integrate a simple UI if desired.

---

## ⚙️ Configuration

All settings are read from `.env`. Common variables you’ll likely see/use:

- `MARKETCAP_API_URL` – your CoinMarketCap API URL
- `MARKETCAP_API_KEY` - your CoinMarketCap API KEY

---

## ❓ How the Data Is Organized (DuckDB)

Typical tables created by the transform phase:

- `main.market_data` – raw ingested rows
- `normalized.coins` – core coin attributes
- `normalized.tags` – one row per (coin, tag)
- `normalized.platform_details` – chain/platform info per coin
- `normalized.quotes` – quotes expanded by currency (USD/EUR/BRL, etc.)
- `analytics.current_prices` – convenience table with latest price & ranking per currency

Example: get the top 10 prices in USD
```sql
SELECT id, name, symbol, price
FROM analytics.current_prices
WHERE currency = 'USD'
ORDER BY price DESC
LIMIT 10;
```

---

## 🖥️ Local (non-Docker) Run

If you prefer running it directly:

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # add your CMC_API_KEY
python main.py
```

---

## 🥊 Troubleshooting

- **No data?** Verify `MARKETCAP_API_KEY` in `.env`, and check logs (`docker compose logs -f`).
- **Permission errors on `/data`?** Ensure your user has write access.
- **Schema missing?** The app should create schemas/tables as needed; if not, confirm your DuckDB path and volume mapping.

In case of any doubt feel free to reach me out!

---

## License

MIT
