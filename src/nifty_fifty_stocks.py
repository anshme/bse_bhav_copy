from constants import (NIFTY_FIFTY_TABLE, SYMBOL, NIFTY_FIFTY, logger, TRADE_DATE,
                       NIFTY_FIFTY_LIST_TABLE, STOCK_TABLE, NIFTY_FIFTY_COL_TYPES)

class NiftyFiftyStocks:
    def __init__(self, con):
        self.con = con
        self._init_nifty_fifty_list_table()
        self._init_nifty_fifty_table()

    def _init_nifty_fifty_list_table(self):
        create_nifty_fifty_list_table_query = f"""
            CREATE TABLE IF NOT EXISTS {NIFTY_FIFTY_LIST_TABLE} (
                {SYMBOL} VARCHAR PRIMARY KEY
            )
        """

        self.con.execute(create_nifty_fifty_list_table_query)
        logger.info("Ensured '%s' table exists", NIFTY_FIFTY_LIST_TABLE)

    def _init_nifty_fifty_table(self):

        columns = ",\n".join(
            [f"{col.lower()} {NIFTY_FIFTY_COL_TYPES.get(col, 'VARCHAR')}" for col in NIFTY_FIFTY_COL_TYPES.keys()]
        )

        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {NIFTY_FIFTY_TABLE} (
                {columns},
                PRIMARY KEY ({SYMBOL.lower()}, {TRADE_DATE.lower()})
            )
        """

        self.con.execute(create_table_query)
        logger.info("Ensured '%s' table exists", NIFTY_FIFTY_TABLE)

    def upsert_stocks_to_nifty_fifty_list(self):
        upsert_query = f"""
            INSERT INTO {NIFTY_FIFTY_LIST_TABLE} ({SYMBOL})
            VALUES (?)
            ON CONFLICT ({SYMBOL}) DO UPDATE SET {SYMBOL}=excluded.{SYMBOL}
        """

        for stock in NIFTY_FIFTY:
            self.con.execute(upsert_query, (stock,))

    def upsert_stocks_to_nifty_fifty_from_all_stocks(self):
        base_columns = [
            c for c in NIFTY_FIFTY_COL_TYPES.keys()
            if not c.upper().endswith("52") and not c.upper().endswith("52_DATE")
            and not c.upper().endswith("4") and not c.upper().endswith("4_DATE")
            and not c.upper().endswith("12") and not c.upper().endswith("12_DATE")
            and not c.upper().endswith("24") and not c.upper().endswith("24_DATE")
        ]

        insert_columns = ", ".join([f"{c.lower()}" for c in base_columns])
        select_columns = ", ".join([f"s.{c.lower()}" for c in base_columns])
        
        nifty50_insert_query = f"""
            INSERT INTO {NIFTY_FIFTY_TABLE} ({insert_columns})
            SELECT {select_columns}
            FROM {STOCK_TABLE} s
            JOIN {NIFTY_FIFTY_LIST_TABLE} n
            ON s.{SYMBOL.lower()} = n.{SYMBOL.lower()}
            ON CONFLICT ({SYMBOL.lower()}, {TRADE_DATE.lower()}) DO NOTHING;
        """
        logger.info("Inserting NIFTY 50 stocks into '%s' table", NIFTY_FIFTY_TABLE)

        self.con.execute(nifty50_insert_query)

    def update_high_and_low(self, weeks: int, overwrite: bool = False):
        if weeks not in [4, 12, 52]:
            raise ValueError("Invalid weeks parameter. Must be one of [4, 12, 52].")
        
        high_col = f"WEEK_HIGH_{str(weeks)}"
        high_date_col = f"WEEK_HIGH_{str(weeks)}_DATE"
        low_col = f"WEEK_LOW_{str(weeks)}"
        low_date_col = f"WEEK_LOW_{str(weeks)}_DATE"

        calculation_query = f"""
            SELECT 
                t1.{SYMBOL},
                t1.{TRADE_DATE},
                MIN(t2.high) AS {low_col},
                MIN_BY(t2.{TRADE_DATE}, t2.high) AS {low_date_col},
                MAX(t2.high) AS {high_col},
                MAX_BY(t2.{TRADE_DATE}, t2.high) AS {high_date_col}
            FROM {NIFTY_FIFTY_TABLE} t1
            JOIN {NIFTY_FIFTY_TABLE} t2
                ON t1.{SYMBOL} = t2.{SYMBOL}
                AND t2.{TRADE_DATE} BETWEEN t1.{TRADE_DATE} - INTERVAL {weeks} WEEK 
                                           AND t1.{TRADE_DATE}
            GROUP BY t1.{SYMBOL}, t1.{TRADE_DATE}
        """

        if overwrite:
            update_query = f"""
                UPDATE {NIFTY_FIFTY_TABLE} as nf
                SET {high_col} = subquery.{high_col},
                    {high_date_col} = subquery.{high_date_col},
                    {low_col} = subquery.{low_col},
                    {low_date_col} = subquery.{low_date_col}
                FROM (
                    {calculation_query}
                ) AS subquery
                WHERE nf.{SYMBOL} = subquery.{SYMBOL}
                  AND nf.{TRADE_DATE} = subquery.{TRADE_DATE}
            """
        else:
            update_query = f"""
                UPDATE {NIFTY_FIFTY_TABLE} as nf
                SET {high_col} = subquery.{high_col},
                    {high_date_col} = subquery.{high_date_col},
                    {low_col} = subquery.{low_col},
                    {low_date_col} = subquery.{low_date_col}
                FROM (
                    {calculation_query}
                ) AS subquery
                WHERE nf.{SYMBOL} = subquery.{SYMBOL}
                  AND nf.{TRADE_DATE} = subquery.{TRADE_DATE}
                  AND (
                        nf.{high_col} IS NULL OR 
                        nf.{high_date_col} IS NULL OR 
                        nf.{low_col} IS NULL OR 
                        nf.{low_date_col} IS NULL
                    )
            """

        logger.info(f"Updating {NIFTY_FIFTY_TABLE} for high and low of {weeks} weeks")
        self.con.execute(update_query)