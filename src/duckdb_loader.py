# duckdb_loader.py
import os
import re
import json
import glob
import logging
from pathlib import Path

import pandas as pd
import duckdb

from duckdb_manager import DuckDBManager
from constants import CSV_FOLDER, DUCKDB_PATH, PARQUET_OUT, ERROR_LOG, CREATE_PARTITIONED_PARQUET, ORDERED_CSV_COLUMNS

# ------------------------
# Logging setup
# ------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("duckdb_loader")

# ------------------------
# Helpers
# ------------------------
def excel_serial_to_timestamp(s):
    """Convert Excel serial (e.g. '45470') to pandas.Timestamp.
       Uses anchor 1899-12-30 which is the common way to convert Excel dates.
    """
    try:
        n = float(s)
        # treat negative/zero or extremely large numbers as invalid
        if n <= 0 or n > 60000:
            return pd.NaT
        return pd.Timestamp('1899-12-30') + pd.to_timedelta(int(n), unit='D')
    except Exception:
        return pd.NaT

def parse_date_string(s):
    """Robust date parser for TIMESTAMP values.
       Handles:
         - '01-APR-2016'  -> %d-%b-%Y
         - '2-Jun-25'     -> %d-%b-%y
         - '45470'        -> Excel serial
         - other common formats via pandas.to_datetime fallback
    """
    if pd.isna(s):
        return pd.NaT
    s = str(s).strip()
    if s == "":
        return pd.NaT

    # If purely numeric (like Excel serial)
    if re.fullmatch(r'\d+(\.0+)?', s):
        return excel_serial_to_timestamp(s)

    # Try explicit known formats first
    fmts = ("%d-%b-%Y", "%d-%b-%y", "%d-%b-%Y %H:%M:%S", "%d-%b-%y %H:%M:%S")
    for fmt in fmts:
        try:
            return pd.to_datetime(s, format=fmt, dayfirst=True)
        except Exception:
            pass

    # Fallback: let pandas infer
    try:
        return pd.to_datetime(s, dayfirst=True, infer_datetime_format=True, errors='coerce')
    except Exception:
        return pd.NaT

def clean_string_columns(df):
    """Trim and uppercase all string columns (object dtype).
       We purposely convert all columns to str first because read_csv used dtype=str.
    """
    for col in df.columns:
        # keep TIMESTAMP as-is
        if col == "TIMESTAMP":
            df[col] = df[col].astype(str).str.strip()
            continue
        # ISIN, SYMBOL, SERIES should be uppercased
        df[col] = df[col].astype(str).str.strip().str.upper()
    return df

def convert_numeric_columns(df):
    # Remove commas and convert numeric fields; allow NaNs for bad values
    numeric_cols = ["OPEN","HIGH","LOW","CLOSE","LAST","PREVCLOSE","TOTTRDQTY","TOTTRDVAL","TOTALTRADES"]
    for col in numeric_cols:
        if col in df.columns:
            # remove commas, and possible currency characters
            df[col] = df[col].astype(str).str.replace(",", "", regex=False).str.replace(" ", "")
            # allow empty strings -> NaN
            df[col] = pd.to_numeric(df[col].replace('', pd.NA), errors='coerce')
    return df

# ------------------------
# Main loader
# ------------------------
def main():
    # sanity checks
    if not os.path.exists(CSV_FOLDER):
        logger.error("CSV folder not found: %s", CSV_FOLDER)
        raise SystemExit(1)

    csv_paths = sorted(glob.glob(os.path.join(CSV_FOLDER, "*.csv")))
    if not csv_paths:
        logger.error("No CSV files found in folder: %s", CSV_FOLDER)
        raise SystemExit(1)

    # connect to persistent DuckDB
    con = DuckDBManager().get_connection()
    logger.info("Connected to DuckDB at %s", DUCKDB_PATH)

    # create table if not exists
    con.execute("""
        CREATE TABLE IF NOT EXISTS stocks (
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
            isin VARCHAR
        );
    """)
    logger.info("Ensured stocks table exists in DuckDB")

    # prepare errors log file header if not exists
    errors_header = [
        "source_file", "row_index", "error_reason", "raw_timestamp", "raw_row_json"
    ]
    if not os.path.exists(ERROR_LOG):
        pd.DataFrame(columns=errors_header).to_csv(ERROR_LOG, index=False)

    total_rows = 0
    total_inserted = 0
    total_errors = 0

    for path in csv_paths:
        fname = os.path.basename(path)
        logger.info("Processing file: %s", fname)

        # read file as strings to avoid parsing surprises
        try:
            df = pd.read_csv(path, dtype=str, header=0, skip_blank_lines=True)
        except Exception as e:
            logger.exception("Failed to read CSV %s — logging as error", fname)
            err_row = {
                "source_file": fname,
                "row_index": -1,
                "error_reason": f"CSV_READ_ERROR: {e}",
                "raw_timestamp": "",
                "raw_row_json": ""
            }
            pd.DataFrame([err_row]).to_csv(ERROR_LOG, index=False, mode="a", header=False)
            total_errors += 1
            continue

        # standardize column names
        df.columns = df.columns.str.strip().str.upper()

        # ensure expected columns present (we'll still attempt with what's available)
        missing = [c for c in ORDERED_CSV_COLUMNS if c not in df.columns]
        if missing:
            logger.warning("File %s missing expected columns: %s", fname, missing)
            # continue processing anyway — missing columns will be treated as NaN/empty

        # Clean strings (uppercase + trim)
        df = clean_string_columns(df)

        # Numeric columns cleaning
        df = convert_numeric_columns(df)

        # Parse dates robustly
        df["PARSED_TRADE_DATE"] = df["TIMESTAMP"].apply(parse_date_string)

        # Determine invalid rows:
        # - SYMBOL missing
        # - PARSED_TRADE_DATE is NaT (unparseable)
        invalid_mask = (
            df.get("SYMBOL", pd.Series([], dtype=object)).isna() |
            (df.get("SYMBOL", pd.Series([], dtype=object)).astype(str).str.strip() == "") |
            df["PARSED_TRADE_DATE"].isna()
        )

        df_invalid = df[invalid_mask].copy()
        df_valid = df[~invalid_mask].copy()

        # Log invalid rows
        if not df_invalid.empty:
            rows_to_log = []
            for idx, row in df_invalid.iterrows():
                reason = []
                if pd.isna(row.get("SYMBOL")) or str(row.get("SYMBOL")).strip() == "":
                    reason.append("MISSING_SYMBOL")
                if pd.isna(row.get("PARSED_TRADE_DATE")):
                    reason.append("INVALID_DATE")
                rows_to_log.append({
                    "source_file": fname,
                    "row_index": int(idx),
                    "error_reason": "|".join(reason),
                    "raw_timestamp": row.get("TIMESTAMP", ""),
                    "raw_row_json": json.dumps(row.drop(labels=["PARSED_TRADE_DATE"]).fillna("").to_dict(), ensure_ascii=False)
                })
            pd.DataFrame(rows_to_log).to_csv(ERROR_LOG, index=False, mode="a", header=False)
            total_errors += len(rows_to_log)
            logger.info("Logged %d bad rows from %s", len(rows_to_log), fname)

        # If no valid rows, continue
        if df_valid.empty:
            logger.info("No valid rows to insert from %s", fname)
            continue

        # Prepare df for insertion with target column names
        # map columns from CSV names to DB schema
        insert_df = pd.DataFrame()
        insert_df["symbol"] = df_valid.get("SYMBOL", pd.NA)
        insert_df["series"] = df_valid.get("SERIES", pd.NA)
        insert_df["open_price"] = df_valid.get("OPEN", pd.NA).astype("float64")
        insert_df["high_price"] = df_valid.get("HIGH", pd.NA).astype("float64")
        insert_df["low_price"] = df_valid.get("LOW", pd.NA).astype("float64")
        insert_df["close_price"] = df_valid.get("CLOSE", pd.NA).astype("float64")
        insert_df["last_price"] = df_valid.get("LAST", pd.NA).astype("float64")
        insert_df["prev_close"] = df_valid.get("PREVCLOSE", pd.NA).astype("float64")
        # TOTTRDQTY and TOTALTRADES may be floats (coerced) — convert to Int if possible; keep nullable
        insert_df["total_traded_qty"] = pd.to_numeric(df_valid.get("TOTTRDQTY", pd.NA), errors="coerce").astype("Int64")
        insert_df["total_traded_val"] = pd.to_numeric(df_valid.get("TOTTRDVAL", pd.NA), errors="coerce").astype("float64")
        insert_df["trade_date"] = pd.to_datetime(df_valid["PARSED_TRADE_DATE"]).dt.date  # store as date
        insert_df["total_trades"] = pd.to_numeric(df_valid.get("TOTALTRADES", pd.NA), errors="coerce").astype("Int64")
        insert_df["isin"] = df_valid.get("ISIN", pd.NA)

        # register and insert into duckdb
        try:
            con.register("tmp_df", insert_df)
            con.execute("""
                INSERT INTO stocks (symbol, series, open_price, high_price, low_price, close_price,
                                      last_price, prev_close, total_traded_qty, total_traded_val,
                                      trade_date, total_trades, isin)
                SELECT symbol, series, open_price, high_price, low_price, close_price,
                       last_price, prev_close, total_traded_qty, total_traded_val,
                       trade_date::DATE, total_trades, isin
                FROM tmp_df;
            """)
            con.unregister("tmp_df")
            inserted = len(insert_df)
            total_inserted += inserted
            total_rows += inserted
            logger.info("Inserted %d rows from %s into DuckDB", inserted, fname)
        except Exception as e:
            logger.exception("Failed to insert data from %s into DuckDB. Logging rows as errors.", fname)
            # Log all rows as failed inserts (so user can inspect)
            rows_to_log = []
            for idx, row in df_valid.iterrows():
                rows_to_log.append({
                    "source_file": fname,
                    "row_index": int(idx),
                    "error_reason": f"DB_INSERT_ERROR: {e}",
                    "raw_timestamp": row.get("TIMESTAMP", ""),
                    "raw_row_json": json.dumps(row.drop(labels=["PARSED_TRADE_DATE"]).fillna("").to_dict(), ensure_ascii=False)
                })
            pd.DataFrame(rows_to_log).to_csv(ERROR_LOG, index=False, mode="a", header=False)
            total_errors += len(rows_to_log)

    # finished processing files
    logger.info("Processing complete. inserted=%d errors=%d", total_inserted, total_errors)

if __name__ == "__main__":
    main()
