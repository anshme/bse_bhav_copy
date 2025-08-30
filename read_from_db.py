from duckdb_manager import DuckDBManager

# Initialize DB connection
db = DuckDBManager("./stocks.duckdb")
con = db.get_connection()

# Query to get count of records per symbol
query = """
SELECT symbol, COUNT(*) AS record_count
FROM stocks
GROUP BY symbol
ORDER BY record_count DESC LIMIT 10;
"""

query2 = """select distinct symbol from stocks where symbol like '%3M%'"""

# Execute query and fetch results
result = con.execute(query).fetchall()

for symbol in result:
    print(symbol)
# Print results nicely
# print("Symbol-wise Record Counts:")
# for symbol, count in result:
#     print(f"{symbol}: {count}")
