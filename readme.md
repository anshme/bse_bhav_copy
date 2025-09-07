# BSE Bhav Copy Data Pipeline

A comprehensive Python-based data pipeline for downloading, processing, and analyzing NSE/BSE stock market data with automated corporate action adjustments.

## 🚀 Features

- **Automated Data Crawling**: Downloads historical stock data from NSE/BSE
- **Corporate Actions Processing**: Handles stock splits, bonus issues, rights issues, and blended rights
- **Price Adjustments**: Automatically adjusts historical prices for corporate actions
- **NIFTY 50 Focus**: Special handling and analysis for NIFTY 50 stocks
- **High-Performance Storage**: Uses DuckDB for fast analytical queries
- **Data Validation**: Built-in data cleaning and validation pipeline
- **Comprehensive Logging**: Tracks all operations and adjustments

## 📁 Project Structure

```
bse_bhav_copy/
├── src/                          # Source code
│   ├── adjust_price.py          # Corporate actions and price adjustments
│   ├── cleaner.py               # Data cleaning utilities
│   ├── constants.py             # Configuration and constants
│   ├── crawler.py               # Web scraping for stock data
│   ├── driver.py                # Main execution script
│   ├── duckdb_manager.py        # Database connection manager
│   ├── nifty_fifty_stocks.py    # NIFTY 50 specific operations
│   └── stocks_pipeline.py       # Data processing pipeline
├── data/
│   ├── Compressed_data/         # Downloaded ZIP files
│   ├── extracted_data/          # Extracted CSV files
│   ├── corporate_action/        # Corporate action data
│   └── ind_nifty500list.csv     # Stock lists
├── DBs/
│   └── nse_stocks.duckdb        # Main database
└── README.md
```

## 🛠️ Installation

### Prerequisites
- Python 3.8+
- pip package manager

### Dependencies
```bash
pip install duckdb pandas requests beautifulsoup4 lxml
```

## 📊 Database Schema

### Main Tables
- **`stocks`**: Raw stock data with OHLC prices, volume, and metadata
- **`nifty_fifty`**: NIFTY 50 stocks with additional metrics (52-week highs/lows, etc.)
- **`nifty_fifty_list`**: List of current NIFTY 50 symbols
- **`applied_actions_log`**: Log of all corporate action adjustments

### Key Columns
- `SYMBOL`: Stock symbol (e.g., "RELIANCE", "TCS")
- `TRADE_DATE`: Trading date
- `OPEN`, `HIGH`, `LOW`, `CLOSE`: Price data
- `TOTTRDQTY`: Total traded quantity
- `WEEK_HIGH_X`, `WEEK_LOW_X`: X-week highs and lows (4, 12, 24, 52 weeks)

## 🚀 Usage

### 1. Data Crawling
```python
from src.driver import crawl_data
crawl_data()  # Downloads latest stock data
```

### 2. Load Historical Data
```python
from src.driver import load_stocks_history_data
load_stocks_history_data(overwrite=False)  # Process CSV files into database
```

### 3. Setup NIFTY 50 Data
```python
from src.driver import load_nifty_fifty_stocks_list_to_db, load_nifty_fifty_stocks_to_db
load_nifty_fifty_stocks_list_to_db()  # Load NIFTY 50 symbols
load_nifty_fifty_stocks_to_db()       # Extract NIFTY 50 data from main stocks table
```

### 4. Update Technical Indicators
```python
from src.driver import update_nifty_fifty_highs_lows
update_nifty_fifty_highs_lows()  # Calculate 4, 12, 24, 52-week highs and lows
```

### 5. Apply Corporate Actions
```python
from src.driver import adjust_price
adjust_price()  # Process all corporate action files and adjust prices
```

### Complete Pipeline
```python
# Run the complete pipeline
from src.driver import *

# 1. Crawl latest data
crawl_data()

# 2. Load data into database
load_stocks_history_data()

# 3. Setup NIFTY 50
load_nifty_fifty_stocks_list_to_db()
load_nifty_fifty_stocks_to_db()

# 4. Calculate technical indicators
update_nifty_fifty_highs_lows()

# 5. Apply corporate actions
adjust_price()
```

## 🔧 Corporate Actions Support

The system automatically handles:

### Stock Splits
- **Face Value Splits**: e.g., "FACE VALUE SPLIT FROM RS.10 TO RS.2"
- Automatically calculates split ratios and adjusts historical prices

### Bonus Issues
- **Bonus Shares**: e.g., "BONUS 1:2" (1 bonus share for every 2 held)
- Adjusts prices to maintain continuity

### Rights Issues
- **Simple Rights**: e.g., "RIGHTS 1:5 @PREMIUM RS.100"
- **Blended Rights**: Multiple rights offers with different ratios and prices
- Calculates theoretical ex-rights price and adjustment factors

### Price Adjustment Formula
```
Adjusted Price = Original Price × Adjustment Factor
```

Where adjustment factors are calculated based on:
- Split Factor = New Face Value / Old Face Value
- Bonus Factor = Old Shares / (Old Shares + New Shares)
- Rights Factor = Ex-Rights Price / Cum-Rights Price

## 📈 Data Analysis Examples

### Query NIFTY 50 Data
```python
import duckdb

con = duckdb.connect('DBs/nse_stocks.duckdb')

# Get latest prices for NIFTY 50
result = con.execute("""
    SELECT symbol, trade_date, close, week_high_52, week_low_52
    FROM nifty_fifty 
    WHERE trade_date = (SELECT MAX(trade_date) FROM nifty_fifty)
    ORDER BY symbol
""").fetchall()
```

### Find Stocks Near 52-Week Highs
```python
result = con.execute("""
    SELECT symbol, close, week_high_52, 
           (close / week_high_52) * 100 as pct_of_high
    FROM nifty_fifty 
    WHERE trade_date = (SELECT MAX(trade_date) FROM nifty_fifty)
    AND (close / week_high_52) > 0.95
    ORDER BY pct_of_high DESC
""").fetchall()
```

## 🔍 Configuration

Key configuration options in `constants.py`:

```python
# Data paths
CSV_FOLDER = "../data/extracted_data"
DUCKDB_PATH = "../DBs/nse_stocks.duckdb"
COMPRESSED_DATA_DIR = "../data/Compressed_data"

# NIFTY 50 stocks list
NIFTY_FIFTY = [
    "ADANIENT", "ADANIPORTS", "APOLLOHOSP", "ASIANPAINT", 
    "AXISBANK", "BAJAJ-AUTO", "BAJFINANCE", "BAJAJFINSV",
    # ... (complete list of 50 stocks)
]

# Supported time periods for highs/lows
SUPPORTED_WEEKS = [4, 12, 24, 52]
```

## 📝 Logging

The system maintains comprehensive logs:
- **Application logs**: `app.log`
- **Error logs**: `load_errors.csv`
- **Parsed files**: `parsed_files.txt`
- **Corporate actions**: `applied_actions_log` table

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📄 License

This project is for educational and research purposes. Please ensure compliance with data provider terms of service.

## ⚠️ Disclaimer

This software is for educational purposes only. The authors are not responsible for any financial decisions made based on this data. Always verify data accuracy and consult financial professionals before making investment decisions.