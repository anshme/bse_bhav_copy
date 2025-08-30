import os
import pandas as pd

from cleaner import Cleaner
from constants import CSV_FOLDER, ERROR_LOG, STAGING_TABLE, STOCK_TABLE, SYMBOL_COLUMN, logger
from datetime import datetime


class StocksPipeline:
    def __init__(self, con):
        self.con = con
        self._init_table()

    def _init_table(self):
        self.con.execute(f"""
            CREATE TABLE IF NOT EXISTS {STOCK_TABLE} (
                symbol VARCHAR,
                series VARCHAR,
                open_price DOUBLE,
                high_price DOUBLE,
                low_price DOUBLE,
                close_price DOUBLE,
                last_price DOUBLE,
                prev_close DOUBLE,
                total_traded_qty BIGINT,
                total_traded_val DOUBLE,
                trade_date DATE,
                total_trades BIGINT,
                isin VARCHAR,
                PRIMARY KEY (symbol, trade_date)
            );
        """)

    @staticmethod
    def _get_file_path(filename):
        if not os.path.exists(CSV_FOLDER):
            logger.error("CSV folder not found: %s", CSV_FOLDER)
            raise FileNotFoundError(f"CSV folder not found: {CSV_FOLDER}")

        file_path = os.path.join(CSV_FOLDER, filename)
        if not os.path.exists(file_path):
            logger.error("%s not found in folder: %s", filename, CSV_FOLDER)
            raise FileNotFoundError(f"{filename} not found in folder: {CSV_FOLDER}")

        return file_path

    def insert_into_db(self, filename):
        file_path = self._get_file_path(filename)

        try:
            df = pd.read_csv(file_path)
            logger.info("Read %d records from %s", len(df), filename)

        except Exception as e:
            logger.exception(f"Failed to read CSV {filename} — logging as error")
            err_row = {
                "source_file": filename,
                "row_index": -1,
                "error_reason": f"CSV_READ_ERROR: {e}",
                "raw_timestamp": "",
                "raw_row_json": "",
                "execution_timestamp": datetime.now().isoformat()
            }
            pd.DataFrame([err_row]).to_csv(ERROR_LOG, index=False, mode="a", header=False)
            return False
        cleaner = Cleaner(df, self.con)
        cleaned_df = cleaner.clean()
        self.load_csv_to_staging(cleaned_df)
        self.print_staging_data()
        logger.info("Cleaned data, %d records remain", len(cleaned_df))
        return True
    
    
    def load_csv_to_staging(self, df):

        df[SYMBOL_COLUMN] = df[SYMBOL_COLUMN].str.strip().str.upper()

        self.con.execute(f"DROP TABLE IF EXISTS {STAGING_TABLE}")

        self.con.register(f"{STAGING_TABLE}", df)

        logger.info("Loaded data into staging table")

    def upsert_into_main(self):
        """Insert only new records into the main table"""
        logger.info("Upserting data into 'stocks' table...")

        query = f"""
            INSERT INTO {STOCK_TABLE}
            SELECT * FROM {STAGING_TABLE}
            ON CONFLICT(symbol, date) DO NOTHING
        """
        self.con.execute(query)

        logger.info("Upsert completed successfully!")