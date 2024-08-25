from data.models.torikano import TorikanoDataProcessor
from data.models.general import DataGatherer, DataStore
from toraniko.styles import factor_mom
import polars as pl
from datetime import datetime as dt
from toraniko.utils import fill_features

data_store = DataStore(base_location='data/local_store', engine="polars")
torikano_data_handler = TorikanoDataProcessor(data_store=data_store)



torikano_data = torikano_data_handler.build_required_data(start_date = dt(2000, 1, 4))


mom_df = factor_mom(torikano_data.select("symbol", "date", "asset_returns"), trailing_days=252, winsor_factor=0.01).collect()

print("-")