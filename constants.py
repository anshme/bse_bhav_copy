import logging
import os
import pandas as pd

CSV_FOLDER = "./extracted_data"     # relative folder containing CSVs
DUCKDB_PATH = "./stocks_test.duckdb"     # persistent duckdb file in current folder
PARQUET_OUT = "./stocks_parquet"    # optional partitioned parquet output
ERROR_LOG = "./load_errors.csv"     # file where bad rows are appended
CREATE_PARTITIONED_PARQUET = True   # set False to skip parquet export
TIMESTAMP_COLUMN = "TIMESTAMP"      # timestamp column name
SYMBOL_COLUMN = "SYMBOL"            # symbol column name
STOCK_TABLE = "stocks"              # main table name
STAGING_TABLE = "staging"           # staging table name
PARSED_TRADE_DATE = "PARSED_TRADE_DATE"

# order in which the fields are present in the CSV, keep PARSED_TRADE_DATE always at end
ORDERED_CSV_COLUMNS = [
    SYMBOL_COLUMN, "SERIES","OPEN","HIGH","LOW","CLOSE","LAST","PREVCLOSE",
    "TOTTRDQTY","TOTTRDVAL", TIMESTAMP_COLUMN,"TOTALTRADES","ISIN", PARSED_TRADE_DATE
]

# Map CSV column types to DuckDB column types
COL_TYPES = {
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

NUMERIC_COLUMNS = [
    "OPEN","HIGH","LOW","CLOSE","LAST","PREVCLOSE","TOTTRDQTY","TOTTRDVAL","TOTALTRADES"
]

ERROR_HEADERS = [
    "source_file", "row_index", "error_reason", "raw_timestamp", "raw_row_json",
    "execution_timestamp"
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("duckdb_loader")

if not os.path.exists(ERROR_LOG):
    pd.DataFrame(columns=ERROR_HEADERS).to_csv(ERROR_LOG, index=False)
