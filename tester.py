# import os
# import pandas as pd

# from cleaner import Cleaner
# path = "C:/Users/anshm/Downloads/bse_bhav_copy/extracted_data/20250602_NSE.csv"
# fname = os.path.basename(path)
# df = pd.read_csv(path, dtype=str, header=0, skip_blank_lines=True)
# cleaner = Cleaner(df, fname)
# df = cleaner.clean()
# print(df.head(5))
from duckdb_manager import DuckDBManager
from stocks_pipeline import StocksPipeline
from constants import DUCKDB_PATH

duckdb_manager = DuckDBManager(DUCKDB_PATH)
duckdb_con = duckdb_manager.get_connection()
stocks = StocksPipeline(duckdb_con)
stocks.insert_into_db("20250603_NSE.csv")
