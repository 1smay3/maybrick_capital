from data.models.torikano import TorikanoDataProcessor
from data.models.general import DataGatherer, DataStore
from toraniko.styles import factor_mom


data_store = DataStore(base_location='data/local_store', engine="polars")
torikano_data_handler = TorikanoDataProcessor(data_store=data_store)



torikano_data = torikano_data_handler.build_required_data()




mom_df = factor_mom(torikano_data.select("symbol", "date", "asset_returns"), trailing_days=252, winsor_factor=0.01).collect()
df_clean = mom_df.drop_nulls(subset=['book_price', 'sales_price'])

print("-")