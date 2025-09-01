import os
from constants import CSV_FOLDER, DUCKDB_PATH
from duckdb_manager import DuckDBManager
from stocks_pipeline import StocksPipeline
from nifty_fifty_stocks import NiftyFiftyStocks
import pandas as pd

duckdb_manager = DuckDBManager(DUCKDB_PATH)
con = duckdb_manager.get_connection()

def load_stocks_history_data():
    stocks_pipeline = StocksPipeline(con)
    for filename in os.listdir(CSV_FOLDER):
        if filename.lower().endswith('.csv'):
            print(filename)
            stocks_pipeline.insert_into_main_db(filename)

def load_nifty_fifty_stocks_list_to_db():
    nifty_fifty_stocks = NiftyFiftyStocks(con)
    nifty_fifty_stocks.upsert_stocks_to_nifty_fifty_list()

def load_nifty_fifty_stocks_to_db():
    nifty_fifty_stocks = NiftyFiftyStocks(con)
    nifty_fifty_stocks.upsert_stocks_to_nifty_fifty_from_all_stocks()

def update_nifty_fifty_highs_lows():
    weeks = [4, 12, 52]
    for week in weeks:
        nifty_fifty_stocks = NiftyFiftyStocks(con)
        nifty_fifty_stocks.update_high_and_low(week, overwrite=True)

update_nifty_fifty_highs_lows()