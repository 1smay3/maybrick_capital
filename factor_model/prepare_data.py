from toraniko.utils import top_n_by_group
import click
import polars as pl
from data.models.general import DataGatherer, DataStore
from _secrets import FMP_API_KEY
from data.models.prices import PricesDataHandler
from data.models.profile import ProfileDataHandler

# Initialize DataHandler
data_store = DataStore(base_location='data/local_store', engine="polars")
data_gatherer = DataGatherer(api_key=FMP_API_KEY, symbols=data_store.symbols, rate_limit=275, data_handler=data_store, max_retries=3)

# Load processed profiles
all_profiles = data_store.read_parquet("processed", "all_profiles")

def generate_sector_dataframe_from_profiles(all_profiles):
    # Get unique sectors
    unique_sectors = all_profiles.select(pl.col("sector").unique()).to_series().to_list()
    unique_symbols  = all_profiles.select(pl.col("symbol").unique()).to_series().to_list()

    all_profiles_no_null = all_profiles.drop_nulls(subset=['sector', 'symbol'])
    all_profiles_no_null = all_profiles_no_null.with_columns(
        pl.lit(1).alias('indicator')
    )


    binary_df = all_profiles_no_null.pivot(
        index='symbol',  # Rows are indexed by 'stock'
        columns='sector',  # Columns are created based on unique values in 'sector'
        values ='indicator'
    )

    binary_df =  binary_df.fill_null(0)

    return binary_df

generate_sector_dataframe_from_profiles(all_profiles)

print("-")

