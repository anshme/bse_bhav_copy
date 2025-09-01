from duckdb_manager import DuckDBManager
from constants import DUCKDB_PATH, NIFTY_FIFTY_TABLE, SYMBOL_COLUMN, PARSED_TRADE_DATE
from tabulate import tabulate

def run_query(query, to_return=False, pretty_print=True):
    # Initialize DB connection
    db = DuckDBManager(DUCKDB_PATH)
    con = db.get_connection()
    cursor = con.execute(query)
    records = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    if to_return:
        return records
    if pretty_print:
        print(tabulate(records, headers=columns, tablefmt="psql"))
    else:
        for record in records:
            print(record)



query = f"SELECT count(distinct nifty_fifty.symbol) FROM nifty_fifty JOIN stocks ON nifty_fifty.symbol = stocks.symbol"
query = f"""SELECT symbol, EXTRACT(year FROM parsed_trade_date) AS year, COUNT(*) 
            FROM nifty_fifty
            GROUP BY symbol, year
            ORDER BY symbol, year; 
        """

symbol = 'NTPC'
date = '2025-08-29'
query = f"""
WITH week_52 AS (
    SELECT symbol, parsed_trade_date, high,
    ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY high desc) as rn
    FROM nifty_fifty
    WHERE symbol = '{symbol}' AND parsed_trade_date >= DATE '{date}' - INTERVAL 52 WEEK
),
min_max AS (
    SELECT MIN(high) AS min_high, 
    MAX(high) AS max_high,
    MIN(parsed_trade_date) AS min_date, 
    MAX(parsed_trade_date) AS max_date
    FROM week_52
)
select min_high, max_high, min_date, max_date from min_max
"""

query = f"""
SELECT 
    t1.symbol,
    t1.parsed_trade_date,
    MIN(t2.high) AS min_high_52w,
    MIN_BY(t2.parsed_trade_date, t2.high) AS min_high_date_52w,
    MAX(t2.high) AS max_high_52w,
    MAX_BY(t2.parsed_trade_date, t2.high) AS max_high_date_52w
FROM nifty_fifty t1
JOIN nifty_fifty t2
  ON t1.symbol = t2.symbol
 AND t2.parsed_trade_date BETWEEN t1.parsed_trade_date - INTERVAL 52 WEEK AND t1.parsed_trade_date
GROUP BY t1.symbol, t1.parsed_trade_date
ORDER BY t1.symbol, t1.parsed_trade_date;
"""

query = "select symbol, parsed_trade_date, high, low, " \
"WEEK_HIGH_52, WEEK_HIGH_52_DATE," \
"WEEK_LOW_52, WEEK_LOW_52_DATE," \
"WEEK_HIGH_12, WEEK_HIGH_12_DATE," \
"WEEK_LOW_12, WEEK_LOW_12_DATE," \
"WEEK_HIGH_4, WEEK_HIGH_4_DATE," \
"WEEK_LOW_4, WEEK_LOW_4_DATE " \
"from nifty_fifty where symbol = 'TITAN' and parsed_trade_date >= '2025-08-01' limit 20"

# query = f"""
# SELECT symbol, parsed_trade_date, high from (
#     SELECT symbol, parsed_trade_date, high,
#     ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY high desc) as rn
#     FROM nifty_fifty
#     WHERE symbol = '{symbol}' AND parsed_trade_date >= DATE '{date}' - INTERVAL 52 WEEK)
# WHERE rn = 1
# """

# query = f"select {SYMBOL_COLUMN}, count(1) from {NIFTY_FIFTY_TABLE} where {PARSED_TRADE_DATE} >= '2025-08-01' group by {SYMBOL_COLUMN} order by count(1) desc"

records = run_query(query)

# Find NIFTY_FIFTY symbols not present in the stocks table
# missing_symbols = []
# for symbol in NIFTY_FIFTY:
#     result = con.execute("SELECT 1 FROM stocks WHERE symbol = ?", [symbol]).fetchone()
#     if result is None:
#         missing_symbols.append(symbol)

# print("Symbols from NIFTY_FIFTY not in stocks table:")
# for symbol in missing_symbols:
#     print(symbol)
# Query to get count of records per symbol
# query = """
# SELECT symbol, COUNT(*) AS record_count
# FROM stocks
# GROUP BY symbol
# ORDER BY record_count DESC LIMIT 10;
# """

# query2 = """select distinct symbol from stocks where symbol like '%3M%'"""

# query = "Describe stocks"
# query = "select count(1) from stocks where parsed_trade_date='2025-08-01'"
# delete_query = "delete from stocks where PARSED_TRADE_DATE='2025-08-01'"
# Execute query and fetch results
# result = con.execute(query).fetchall()
# for row in result:
#     print(row)

# con.execute(delete_query)
# result = con.execute(query).fetchall()

# for row in result:
#     print(row)
# Print results nicely
# print("Symbol-wise Record Counts:")
# for symbol, count in result:
#     print(f"{symbol}: {count}")
