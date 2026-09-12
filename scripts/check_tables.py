import sqlite3

conn = sqlite3.connect(r"C:\Users\Hp\Desktop\steam-analytics-pipeline\warehouse\steam_star_schema.db")
tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
print(tables)
conn.close()