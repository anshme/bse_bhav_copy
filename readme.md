# cleaner.py

"""
cleaner.py

This module provides functions and classes for cleaning and preprocessing data.
It includes utilities for handling missing values, normalizing data, and removing
unwanted characters or outliers from datasets. The functions are designed to be
used as part of a data pipeline to ensure data quality before analysis or storage.

Typical usage:
    from cleaner import clean_dataframe

    cleaned_df = clean_dataframe(raw_df)
"""

# constants.py

"""
constants.py

This module defines constant values used throughout the project. These constants
may include configuration parameters, default values, file paths, environment
variables, and other fixed values that should not be changed during runtime.

Typical usage:
    from constants import DEFAULT_DB_PATH, MAX_RETRIES
"""

# duckdb_manager.py

"""
duckdb_manager.py

This module provides a manager class and related functions for interacting with
DuckDB databases. It handles database connections, query execution, transaction
management, and utility operations such as creating tables or importing/exporting data.

Typical usage:
    from duckdb_manager import DuckDBManager

    db = DuckDBManager('my_database.db')
    db.execute_query("SELECT * FROM users")
"""