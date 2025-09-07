from datetime import datetime
from fileinput import filename
import json
import pandas as pd
import re
from constants import (ERROR_HEADERS, ERROR_LOG, 
                       SYMBOL, TIMESTAMP_COLUMN, 
                       NUMERIC_COLUMNS, TRADE_DATE,
                       logger)


class Cleaner:
    def __init__(self):
        pass

    def clean(self, df, filename):
        df = self.clean_string_columns(df)
        df = self.convert_numeric_columns(df)
        df = self.drop_missing_pks(df, filename)
        df[TRADE_DATE] = df[TIMESTAMP_COLUMN].apply(Cleaner.parse_date_string)
        df = df.drop_duplicates(subset=[SYMBOL, TRADE_DATE]).copy()
        return df
    
    def drop_missing_pks(self, df, filename):
        
        execution_timestamp = datetime.now().isoformat()
        error_file = ERROR_LOG[:-4] + f"_{execution_timestamp}.csv"
        missing_timestamp_mask = (
            df[TIMESTAMP_COLUMN].isna() | (df[TIMESTAMP_COLUMN].str.strip().str.lower().isin(['', 'nan']))
        )

        match = re.match(r"(\d{8})_", filename)
        if match:
            date_str = match.group(1)
            # Convert YYYYmmdd to DD-MMM-YYYY (e.g., 20210623 -> 23-JUN-2021)
            try:
                parsed_date = pd.to_datetime(date_str, format="%Y%m%d")
                formatted_date = parsed_date.strftime("%d-%b-%Y").upper()
                # Fill only missing TIMESTAMPs
                df.loc[missing_timestamp_mask, TIMESTAMP_COLUMN] = formatted_date
            except Exception:
                logger.warning("Failed to parse date from filename: %s", filename)
                pass  # If parsing fails, skip filling

        # Now drop rows with missing SYMBOL or TIMESTAMP
        missing_mask = (
            df[SYMBOL].isna() | (df[SYMBOL].str.strip().str.lower().isin(['', 'nan']))
            | df[TIMESTAMP_COLUMN].isna() | (df[TIMESTAMP_COLUMN].str.strip().str.lower().isin(['', 'nan']))
        )
        if missing_mask.any():
            dropped = df[missing_mask].copy()
            dropped_rows = []
            for idx, row in dropped.iterrows():
                dropped_rows.append({
                    "source_file": filename,
                    "row_index": idx,
                    "error_reason": "MISSING_PK",
                    "raw_timestamp": row.get(TIMESTAMP_COLUMN, ""),
                    "raw_row_json": json.dumps(row.to_dict(), default=str),
                    "execution_timestamp": datetime.now().isoformat()
                })
            pd.DataFrame(dropped_rows, columns=ERROR_HEADERS).to_csv(
                error_file, mode="a", index=False, header=False
            )
            logger.info("Logged %d dropped records from %s", len(dropped_rows), filename)
            df = df[~missing_mask].copy()
        return df

    def clean_string_columns(self, df):
        """
        Trim and uppercase all string columns (object dtype).
        We purposely convert all columns to str first because read_csv used dtype=str.
        """
        for col in df.columns:
            # keep TIMESTAMP as-is
            if col == TIMESTAMP_COLUMN:
                df[col] = df[col].astype(str).str.strip()
                continue
            # ISIN, SYMBOL, SERIES should be uppercased
            df[col] = df[col].astype(str).str.strip().str.upper()
        return df
    
    def convert_numeric_columns(self, df):
        # Remove commas and convert numeric fields; allow NaNs for bad values

        for col in NUMERIC_COLUMNS:
            if col in df.columns:
                # remove commas, and possible currency characters
                df[col] = df[col].astype(str).str.replace(",", "", regex=False).str.replace(" ", "")
                # allow empty strings -> NaN
                df[col] = pd.to_numeric(df[col].replace('', pd.NA), errors='coerce')
        return df
    
    @staticmethod
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
            return Cleaner.excel_serial_to_timestamp(s)

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
    
    @staticmethod
    def excel_serial_to_timestamp(s):
        """
        Convert Excel serial (e.g. '45470') to pandas.Timestamp.
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
        