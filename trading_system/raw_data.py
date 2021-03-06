"""This file behavas as dummy trading system.
"""
import pandas as pd
import numpy as np


def get_raw_data(dt_from: pd.Timestamp, dt_to: pd.Timestamp) -> pd.DataFrame:
    """
    Generates (random) trade data
    :param dt_from (pd.Timestamp): datetime from which to load the data
    :param dt_to (pd.Timestamp): datetime up to which to load the data
    """

    n_rows = int(1e6)
    min_price = -100
    max_price = 300
    min_vol = -100
    max_vol = 100
    cols = [
        "trade_id",  # integer string identifying trade
        "execution_time",  # time at which the trade was executed
        "price",  # price in EUR/MWh 
        "volume",  # volume in MWh (negative = buy, positive = sell)
        "market",  # country
        "product"  # name of the product
    ]
    markets = ["uk", "nl", "de"]
    products = ["XBID_2h", "XBID_1h", "XBID_30min", "local_2h", "local_1h", "local_30min"]

    df_raw = pd.DataFrame(index=range(n_rows), columns=cols)
    df_raw["product"] = np.random.choice(products, size=n_rows)
    df_raw["market"] = np.random.choice(markets, size=n_rows)
    df_raw["execution_time"] = pd.to_datetime(
        np.random.uniform(
            dt_from.to_numpy(), 
            dt_to.to_numpy(), 
            size=n_rows
        )
    ).round("1ms")
    df_raw["price"] = np.random.uniform(
        min_price, 
        max_price, 
        size=n_rows
    )
    df_raw["volume"] = np.random.uniform(
        min_vol, 
        max_vol, 
        size=n_rows
    )
    df_raw["trade_id"] = np.random.randint(n_rows, n_rows*2, size=n_rows).astype(str)
    df_raw["product_duration"] = df_raw["product"].str.split("_").str[-1]
    df_raw["product_type"] = df_raw["product"].str.split("_").str[0]

    return df_raw
