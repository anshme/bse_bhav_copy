import pandas as pd
import re
from constants import TIMESTAMP_COLUMN, NUMERIC_COLUMNS


class Cleaner:
    def __init__(self, df, filename):
        self.df = df
        self.filename = filename

    def clean(self):
        self.df = self.clean_string_columns(self.df)
        self.df = self.convert_numeric_columns(self.df)
        self.df["PARSED_TRADE_DATE"] = self.df[TIMESTAMP_COLUMN].apply(Cleaner.parse_date_string)
        return self.df

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
        