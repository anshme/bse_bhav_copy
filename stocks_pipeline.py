import os
import pandas as pd

from cleaner import Cleaner
from constants import (CSV_FOLDER, ERROR_LOG, 
                       STAGING_TABLE, STOCK_TABLE, STOCK_TABLE_COL_TYPES,
                       SYMBOL_COLUMN, TIMESTAMP_COLUMN, logger, 
                       ORDERED_CSV_COLUMNS, PARSED_TRADE_DATE)
from datetime import datetime


class StocksPipeline:
    def __init__(self, con):
        self.con = con
        self._init_table()

    def _init_table(self):
        columns = ",\n".join(
            [f"{col.lower()} {STOCK_TABLE_COL_TYPES.get(col, 'VARCHAR')}" for col in ORDERED_CSV_COLUMNS]
        )

        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {STOCK_TABLE} (
                {columns},
                PRIMARY KEY ({SYMBOL_COLUMN.lower()}, {PARSED_TRADE_DATE.lower()})
            )
        """

        self.con.execute(create_table_query)
        logger.info("Ensured '%s' table exists", STOCK_TABLE)


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

    def read_from_csv(self, file_path):
        try:
            df = pd.read_csv(file_path)
            logger.info("Read %d records from %s", len(df), file_path)
            return df
        except Exception as e:
            logger.exception(f"Failed to read CSV {file_path} — logging as error")
            err_row = {
                "source_file": file_path,
                "row_index": -1,
                "error_reason": f"CSV_READ_ERROR: {e}",
                "raw_timestamp": "",
                "raw_row_json": "",
                "execution_timestamp": datetime.now().isoformat()
            }
            pd.DataFrame([err_row]).to_csv(ERROR_LOG, index=False, mode="a", header=False)
            return pd.DataFrame()  # Return an empty DataFrame on error

    def insert_into_main_db(self, filename):
        file_path = self._get_file_path(filename)

        df = self.read_from_csv(file_path)
        if df.empty:
            logger.error("No valid data found in %s", filename)
            return False

        logger.info("Read %d records from %s", len(df), filename)

        cleaner = Cleaner()
        cleaned_df = cleaner.clean(df, filename)
        self.load_csv_to_staging(cleaned_df)
        logger.info("Cleaned data, %d records remain", len(cleaned_df))
        self.upsert_into_main()
        return True
    
    def print_staging_data(self, limit=5):
        """Prints the first few rows of the staging table for inspection."""
        try:
            query = f"SELECT * FROM {STAGING_TABLE} LIMIT {limit}"
            df = self.con.execute(query).fetchdf()
            print(f"\n--- {STAGING_TABLE} (showing up to {limit} rows) ---")
            print(df)
        except Exception as e:
            logger.error("Failed to print staging data: %s", e)

    def load_csv_to_staging(self, df):

        df[SYMBOL_COLUMN] = df[SYMBOL_COLUMN].str.strip().str.upper()

        self.con.execute(f"DROP VIEW IF EXISTS {STAGING_TABLE}")
        self.con.execute(f"DROP TABLE IF EXISTS {STAGING_TABLE}")

        self.con.register(f"{STAGING_TABLE}", df)

        logger.info("Loaded data into staging table")

    def upsert_into_main(self):
        """Insert only new records into the main table"""
        logger.info("Upserting data into 'stocks' table...")

        update_columns = [c.lower() for c in ORDERED_CSV_COLUMNS 
                          if c not in [SYMBOL_COLUMN, TIMESTAMP_COLUMN]]
        
        insert_columns = ", ".join([c.lower() for c in ORDERED_CSV_COLUMNS])
        update_columns = [c.lower() for c in ORDERED_CSV_COLUMNS if c not in [SYMBOL_COLUMN, PARSED_TRADE_DATE]]
        set_clause = ",\n".join([f"{col} = excluded.{col}" for col in update_columns])

        query = f"""
            INSERT INTO {STOCK_TABLE} ({insert_columns})
            SELECT {insert_columns} FROM {STAGING_TABLE}
            ON CONFLICT ({SYMBOL_COLUMN.lower()}, {PARSED_TRADE_DATE.lower()}) DO UPDATE SET
                {set_clause}
        """

        try:
            self.con.execute(query)

        except Exception as e:
            logger.info("Upsert query:\n%s", query)
            logger.error("Failed to upsert data into 'stocks' table: %s", e)
            raise Exception(f"Upsert failed: {e}")

        logger.info("Upsert completed successfully!")