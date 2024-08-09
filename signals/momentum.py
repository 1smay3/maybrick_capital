def simple_trend_signal(daily_returns, slow_lookback, fast_lookback, vol_lookback):
    slow = daily_returns.cumsum().ewm(span =slow_lookback).mean()
    fast = daily_returns.cumsum().ewm(span =fast_lookback).mean()

    vol = daily_returns.ewm(span=vol_lookback).std()

    mom = (fast-slow)/vol

    return mom


# Load total returns
from data.storage import DataHandler  # Assume this is the module where your DataHandler class is defined

data_handler = DataHandler(folder_name="local_store")

tr = data_handler.read_parquet("total_return", engine="pandas").set_index("date")


LOOKBACKS_MONTHS = [1, 3, 6, 12]

trend_signals = {}
for lookback in LOOKBACKS_MONTHS:
    num_days = lookback * 20
    trend_signals[str(lookback)] = simple_trend_signal(tr, num_days, num_days/2, 60)

print("-")