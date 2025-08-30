import duckdb
import os
import atexit
import threading

class DuckDBManager:
    """
    A singleton-style DuckDB connection manager.
    - Creates a single connection to the DuckDB database.
    - Reuses the same connection when requested.
    - Closes the connection automatically when the program exits.
    """

    _instance = None
    _lock = threading.Lock()

    def __new__(cls, db_path: str = "database.duckdb"):
        # Ensure thread-safe singleton creation
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(DuckDBManager, cls).__new__(cls)
                cls._instance._init_connection(db_path)
        return cls._instance

    def _init_connection(self, db_path: str):
        """Initialize the DuckDB connection."""
        self.db_path = os.path.abspath(db_path)
        self._con = duckdb.connect(self.db_path)
        print(f"✅ Connected to DuckDB at: {self.db_path}")

        # Register exit handler to close connection automatically
        atexit.register(self.close_connection)

    def get_connection(self):
        """Return the existing DuckDB connection."""
        return self._con

    def close_connection(self):
        """Close the DuckDB connection if open."""
        if hasattr(self, "_con") and self._con:
            try:
                self._con.close()
                print("🔒 DuckDB connection closed.")
            except Exception as e:
                print(f"⚠️ Error closing DuckDB connection: {e}")
            finally:
                self._con = None