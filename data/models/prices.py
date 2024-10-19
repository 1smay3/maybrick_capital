# PricesDataHandler.py
import polars as pl
from data.models.general import GenericDataHandler
from data.utils import pct_change
from constants import FLOAT_FIELDS_PRICES, DATA_START_DATE

class PricesDataHandler(GenericDataHandler):
    def __init__(self, data_gatherer, data_store, interval, sub_directory):
        super().__init__(data_gatherer, data_store, sub_directory)
        self.interval = interval
        self.api_key = data_gatherer.api_key
        self.endpoint_url = "https://financialmodelingprep.com/api/v3/{interval}/{symbol}?from=1900-01-01&apikey={api_key}"

    def build_url(self, symbol):
        """Build the URL for fetching data."""
        return self.endpoint_url.format(
            interval=self.interval, symbol=symbol, api_key=self.api_key
        )

    def process_raw_prices(self, data):
        """Process the raw prices data."""
        for record in data.get("historical", []):
            for field in FLOAT_FIELDS_PRICES:
                record[field] = float(record.get(field, 0))
        df = pl.DataFrame(data["historical"])
        df = df.with_columns(pl.col("date").str.strptime(pl.Datetime))
        return df

    def _process_data(self, data):
        return self.process_raw_prices(data)

    def _build_adj_close_frame(self, key):
        """Build a frame for adjusted close prices."""
        field = "adjClose"
        prices_df = self.get_field(key, field)
        sorted_df = prices_df.sort(by="date")
        self.data_store.write_parquet(sorted_df, "processed/market_data", "prices.parquet", metadata=None)
        self.data_cache["processed_prices"] = sorted_df

    def _generate_total_returns(self):
        processed_prices = self.data_cache["processed_prices"]
        total_returns = pct_change(processed_prices, lookback=1)
        self.data_store.write_parquet(total_returns, "processed/market_data", "total_return.parquet")
        self.data_cache["total_return"] = total_returns

    def build_processed_prices(self, key):
        self._build_adj_close_frame(key)
        self._generate_total_returns()


    def _generate_total_returns(self):
        processed_prices = self.data_cache["processed_prices"]
        # Calculate total returns
        total_returns = pct_change(processed_prices, lookback=1)  # Calculate pct_change over 1 period
        # Save total returns
        self.data_store.write_parquet(total_returns,  "processed/market_data", "total_return.parquet", metadata=None)
        # Also add to cache to pick up later
        self.data_cache["total_return"] = total_returns

    def build_processed_prices(self, key):
        self._build_adj_close_frame(key)
        self._generate_total_returns()

    def build_base_frame(self, start_date = DATA_START_DATE):
        # Builds a base dataframe that everything is reindexed by to keep everything the same shape
        # Load total returns
        total_returns = self.data_store.read_parquet("processed/market_data", "total_return.parquet")

        total_returns_sliced = total_returns.filter(pl.col('date') >= start_date)


        filtered_df = total_returns_sliced.filter(
            pl.any_horizontal(pl.col(pl.Float32, pl.Float64).is_not_nan())
        )

        self.data_store.write_parquet(filtered_df,  "core_data", "base_frame.parquet", metadata=None)

        return filtered_df
