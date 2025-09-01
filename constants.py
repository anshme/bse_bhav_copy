import logging
import os
import pandas as pd

CSV_FOLDER = "./extracted_data"          # relative folder containing CSVs
DUCKDB_PATH = "./stocks_test.duckdb"     # persistent duckdb file in current folder
ERROR_LOG = "./load_errors.csv"          # file where bad rows are appended

TIMESTAMP_COLUMN = "TIMESTAMP"           # timestamp column name
SYMBOL_COLUMN = "SYMBOL"                 # symbol column name
PARSED_TRADE_DATE = "PARSED_TRADE_DATE"  # parsed trade date column name

STOCK_TABLE = "stocks"                   # main table name
STAGING_TABLE = "staging"                # staging table name
NIFTY_FIFTY_LIST_TABLE = "nifty_fifty_list"        # NIFTY 50 stocks list table name
NIFTY_FIFTY_TABLE = "nifty_fifty"        # NIFTY 50 stocks table name

# order in which the fields are present in the CSV, keep PARSED_TRADE_DATE always at end
ORDERED_CSV_COLUMNS = [
    SYMBOL_COLUMN, "SERIES","OPEN","HIGH","LOW","CLOSE","LAST","PREVCLOSE",
    "TOTTRDQTY","TOTTRDVAL", TIMESTAMP_COLUMN,"TOTALTRADES","ISIN", PARSED_TRADE_DATE
]

# Map CSV column types to DuckDB column types
STOCK_TABLE_COL_TYPES = {
        SYMBOL_COLUMN: "VARCHAR",
        "SERIES": "VARCHAR",
        "OPEN": "DOUBLE",
        "HIGH": "DOUBLE",
        "LOW": "DOUBLE",
        "CLOSE": "DOUBLE",
        "LAST": "DOUBLE",
        "PREVCLOSE": "DOUBLE",
        "TOTTRDQTY": "BIGINT",
        "TOTTRDVAL": "DOUBLE",
        TIMESTAMP_COLUMN: "VARCHAR",
        "TOTALTRADES": "BIGINT",
        "ISIN": "VARCHAR",
        PARSED_TRADE_DATE: "DATE"
    }

NIFTY_FIFTY_COL_TYPES = {
        SYMBOL_COLUMN: "VARCHAR",
        "SERIES": "VARCHAR",
        "OPEN": "DOUBLE",
        "HIGH": "DOUBLE",
        "LOW": "DOUBLE",
        "CLOSE": "DOUBLE",
        "LAST": "DOUBLE",
        "PREVCLOSE": "DOUBLE",
        "TOTTRDQTY": "BIGINT",
        "TOTTRDVAL": "DOUBLE",
        "TOTALTRADES": "BIGINT",
        "ISIN": "VARCHAR",
        PARSED_TRADE_DATE: "DATE",
        "WEEK_HIGH_52": "DOUBLE",
        "WEEK_HIGH_52_DATE": "DATE",
        "WEEK_LOW_52": "DOUBLE",
        "WEEK_LOW_52_DATE": "DATE",
        "WEEK_HIGH_4": "DOUBLE",
        "WEEK_HIGH_4_DATE": "DATE",
        "WEEK_LOW_4": "DOUBLE",
        "WEEK_LOW_4_DATE": "DATE",
        "WEEK_HIGH_12": "DOUBLE",
        "WEEK_HIGH_12_DATE": "DATE",
        "WEEK_LOW_12": "DOUBLE",
        "WEEK_LOW_12_DATE": "DATE"
    }

NUMERIC_COLUMNS = [
    "OPEN","HIGH","LOW","CLOSE","LAST","PREVCLOSE","TOTTRDQTY","TOTTRDVAL","TOTALTRADES"
]



NIFTY_FIFTY = [
    "ADANIENT", "ADANIPORTS", "APOLLOHOSP", "ASIANPAINT", "AXISBANK", "BAJAJ-AUTO", "BAJFINANCE", "BAJAJFINSV", "BEL",
    "BHARTIARTL", "CIPLA", "COALINDIA", "DRREDDY", "EICHERMOT", "ETERNAL", "GRASIM", "HCLTECH", "HDFCBANK", "HDFCLIFE",
    "HEROMOTOCO", "HINDALCO", "HINDUNILVR", "ICICIBANK", "ITC", "INDUSINDBK", "INFY", "JSWSTEEL", "JIOFIN", "KOTAKBANK",
    "LT", "M&M", "MARUTI", "NTPC", "NESTLEIND", "ONGC", "POWERGRID", "RELIANCE", "SBILIFE", "SHRIRAMFIN", "SBIN",
    "SUNPHARMA", "TCS", "TATACONSUM", "TATAMOTORS", "TATASTEEL", "TECHM", "TITAN", "TRENT", "ULTRACEMCO", "WIPRO"
]

ERROR_HEADERS = [
    "source_file", "row_index", "error_reason", "raw_timestamp", "raw_row_json",
    "execution_timestamp"
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("duckdb_loader")

if not os.path.exists(ERROR_LOG):
    pd.DataFrame(columns=ERROR_HEADERS).to_csv(ERROR_LOG, index=False)
