import os
from constants import CSV_FOLDER, DUCKDB_PATH
from duckdb_manager import DuckDBManager
from stocks_pipeline import StocksPipeline
import pandas as pd

duckdb_manager = DuckDBManager(DUCKDB_PATH)
con = duckdb_manager.get_connection()
stocks_pipeline = StocksPipeline(con)

for filename in os.listdir(CSV_FOLDER):
    if filename.lower().endswith('.csv'):
        print(filename)
        stocks_pipeline.insert_into_db(filename)
