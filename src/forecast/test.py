import requests
import numpy as np
import pandas as pd
import time

def fetch_feature_window():
    tik = time.time()
    query = """
        SELECT bid, bid_qty, ask, ask_qty, last,
               volume, vwap, low, high, change, change_pct, ma_5,
               ma_14, ema_5, std_14, price_change, price_change_pct,
               max_14, min_14, vwap_diff, bid_ask_spread, log_return,
               momentum, volatility, cumulative_volume, volume_change,
               mean_bid_qty, mean_ask_qty 
        FROM gold_1min
        ORDER BY timestamp;
    """
    url = "http://localhost:9000/exec"
    res = requests.get(url, params={"query": query})
    json_data = res.json()

    # Extract column names
    column_names = [col["name"] for col in json_data["columns"]]
    rows = json_data["dataset"]

    # Convert to DataFrame
    df = pd.DataFrame(rows, columns=column_names)

    tok = time.time()
    print(f"Time taken to fetch data: {tok - tik:.2f} seconds")
    return df


if __name__ == "__main__":
    df = fetch_feature_window()
    print(df.head())
