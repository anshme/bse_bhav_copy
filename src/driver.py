import os
from constants import CSV_FOLDER, DUCKDB_PATH, PARSED_FILES, TRADE_DATE, STOCK_TABLE, CRAWLED_TILL_DATE_TABLE, LAST_CRAWLED_DATE
from duckdb_manager import DuckDBManager
from stocks_pipeline import StocksPipeline
from nifty_fifty_stocks import NiftyFiftyStocks
import pandas as pd
from adjust_price import GeneralMeeting

duckdb_manager = DuckDBManager(DUCKDB_PATH)
con = duckdb_manager.get_connection()

def load_stocks_history_data(overwrite=False):
    with open(PARSED_FILES, "r") as f:
        parsed_csv_files = [line.strip() for line in f if line.strip()]
    
    all_csv_files = os.listdir(CSV_FOLDER)
    csv_files_to_process = None
    if overwrite:
        csv_files_to_process = [f for f in all_csv_files if f.lower().endswith('.csv')]
    else:
        csv_files_to_process = [f for f in all_csv_files if f.lower().endswith('.csv') and f not in parsed_csv_files]

    stocks_pipeline = StocksPipeline(con)
    mode = "w" if overwrite else "a"
    with open(PARSED_FILES, mode) as f:
        for filename in csv_files_to_process:
            stocks_pipeline.insert_into_stocks_db(filename)
            f.write(f"{filename}\n")
    
    result = con.execute(f"SELECT MAX({TRADE_DATE.lower()}) FROM {STOCK_TABLE}").fetchone()
    if result and result[0]:
        latest_date = result[0]
        con.execute(f"""
            INSERT INTO {CRAWLED_TILL_DATE_TABLE} ({LAST_CRAWLED_DATE})
            VALUES (?)
            ON CONFLICT ({LAST_CRAWLED_DATE}) DO UPDATE SET
                {LAST_CRAWLED_DATE} = excluded.{LAST_CRAWLED_DATE}
        """, (latest_date,))
        print(f"Updated {CRAWLED_TILL_DATE_TABLE} with latest crawled date: {latest_date}")

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

def adjust_price():
    gm = GeneralMeeting(con)
    all_csv_files = os.listdir("../data/corporate_action/")
    for file in all_csv_files:
        file_path = f"../data/corporate_action/{file}"
        if file_path.endswith(".csv"):
            gm.adjust_price(file_path)

def crawl_data():
    from crawler import Crawler
    crawler = Crawler()
    query = "SELECT MAX({}) FROM {}".format(LAST_CRAWLED_DATE, CRAWLED_TILL_DATE_TABLE)
    result = con.execute(query).fetchone()
    last_crawled_date = str(result[0]) if result else None
    to_date = pd.Timestamp.now().strftime("%Y-%m-%d").upper()
    if last_crawled_date and last_crawled_date <= to_date:
        print(f"Crawled date is {last_crawled_date}")
        print(f"Crawling date till {to_date}")
        crawler.crawl(last_crawled_date, to_date)
    else:
        print(f"No new data to crawl.\nLatest crawled date -> {last_crawled_date}\nToday's date -> {to_date}")

# crawl_data()
# load_stocks_history_data(overwrite=False)
# load_nifty_fifty_stocks_list_to_db()
# load_nifty_fifty_stocks_to_db()
# adjust_price()


gm = GeneralMeeting(con)
all_csv_files = os.listdir("../data/corporate_action/")
for file in all_csv_files:
    file_path = f"../data/corporate_action/{file}"
    if file_path.endswith(".csv"):
        # details = gm.get_actions_from_csv(file_path)
        details = gm.adjust_price(file_path)
        print(details)