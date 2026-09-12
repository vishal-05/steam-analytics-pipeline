import sqlite3
import pandas as pd

conn = sqlite3.connect(r"C:\Users\Hp\Desktop\steam-analytics-pipeline\warehouse\steam_star_schema.db")

dim_date = pd.read_sql("SELECT * FROM dim_date", conn)
dim_publisher = pd.read_sql("SELECT * FROM dim_publisher", conn)
dim_game = pd.read_sql("SELECT * FROM dim_game", conn)
fact_game_performance = pd.read_sql("SELECT * FROM fact_game_performance", conn)

conn.close()

output_dir = r"C:\Users\Hp\Desktop\steam-analytics-pipeline\warehouse\gold"

dim_date.to_csv(f"{output_dir}\\dim_date.csv", index=False)
dim_publisher.to_csv(f"{output_dir}\\dim_publisher.csv", index=False)
dim_game.to_csv(f"{output_dir}\\dim_game.csv", index=False)
fact_game_performance.to_csv(f"{output_dir}\\fact_game_performance.csv", index=False)

print("All tables exported to CSV.")