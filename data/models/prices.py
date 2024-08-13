import asyncio
from data.utils import pct_change
import polars as pl
from collections import defaultdict
from constants import FLOAT_FIELDS_PRICES


class PricesDataHandler:
    def __init__(self, data_gatherer, data_store, interval, sub_directory):
        self.data_gatherer = data_gatherer
        self.data_store = data_store
        self.interval = interval
        self.api_key = data_gatherer.api_key
        self.endpoint_url = 'https://financialmodelingprep.com/api/v3/{interval}/{symbol}?from=1900-01-01&apikey={api_key}'
        self.sub_directory = sub_directory  # Define the subdirectory for storing price data
        self.data_cache = defaultdict(pl.DataFrame)  # To store and access data by key

    def build_url(self, symbol):
        """Build the URL for fetching data."""
        return self.endpoint_url.format(interval=self.interval, symbol=symbol, api_key=self.api_key)

    def __process_raw_prices(self, data):
        for record in data.get('historical', []):
            for field in FLOAT_FIELDS_PRICES:
                record[field] = float(record.get(field, 0))
            # Create and return Polars DataFrame
        df = pl.DataFrame(data['historical'])
        # Ensure correct date parsing
        df = df.with_columns(pl.col('date').str.strptime(pl.Datetime))
        return df

    def _process_raw_mktcap(self, data):
        df = pl.DataFrame(data)
        df = df.with_columns(pl.col('date').str.strptime(pl.Datetime))
        return df

    def _process_data(self, data):
        if self.sub_directory == "prices":
            response = self.__process_raw_prices(data)
        if self.sub_directory == "marketcap":
            response = self._process_raw_mktcap(data)
        return response



    async def gather_and_store_data(self):
        """Fetch data for all symbols and store it."""
        await self.data_gatherer._fetch_all_data(self.build_url, self._process_data, self.sub_directory)

    def update_data(self):
        """Run the async gathering and storing process."""
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                raise RuntimeError("Cannot run 'update_data' while another event loop is running")
            else:
                loop.run_until_complete(self.gather_and_store_data())
        except RuntimeError as e:
            print(f"RuntimeError: {e}")
            raise e

    def read_raw_data(self, sub_directory):
        """Load raw data from the data store and cache it."""
        all_data = self.data_store.read_all(sub_directory)
        # Cache data using sub_directory as key
        self.data_cache[sub_directory] = all_data
        return all_data

    def _get_list_of_field_frames(self, key, field):
        """Fetch and process data for a specific field using cached data."""
        if key not in self.data_cache:
            raise ValueError(f"No data available for key: {key}")

        all_frames = self.data_cache[key]

        dfs = []
        for frame_name, frame_data in all_frames.items():
            symbol = frame_name.split('_')[1]
            data = frame_data[["date", field]].rename({field: symbol})
            dfs.append(data)
        return dfs


    def get_field(self, key, field):
        """Get a merged DataFrame of a specific field across all symbols."""
        list_of_frames = self._get_list_of_field_frames(key, field)
        if not list_of_frames:
            return pl.DataFrame()  # Return an empty DataFrame if no frames available
        merged_df = list_of_frames[0]
        for df in list_of_frames[1:]:
            merged_df = merged_df.join(df, on="date", how="outer", coalesce=True)
        return merged_df

    def _build_adj_close_frame(self, key):
        if key not in self.data_cache:
            raise ValueError(f"No data available for key: {key}")


        field = "adjClose"  # Example field name; adjust as needed
        prices_df = self.get_field(key, field)
        # Sort by the 'date' column in ascending order
        sorted_df = prices_df.sort(by="date")

        self.data_store.write_parquet(sorted_df, "processed", "prices")
        # Also add to cache to pick up later as its 'always' going to be
        self.data_cache["processed_prices"] = sorted_df


    def _generate_total_returns(self):
        processed_prices = self.data_cache["processed_prices"]
        # Calculate total returns
        total_returns = pct_change(processed_prices, lookback=1)  # Calculate pct_change over 1 period
        # Save total returns
        self.data_store.write_parquet(total_returns, "processed", "total_return")
        # Also add to cache to pick up later
        self.data_cache["total_return"] = total_returns

    def build_processed_prices(self, key):
        self._build_adj_close_frame(key)
        self._generate_total_returns()

    # TODO: process market cap
    def build_processed_market_caps(self, key):
        if key not in self.data_cache:
            raise ValueError(f"No data available for key: {key}")

        field = "marketCap"  # Example field name; adjust as needed
        market_cap_df = self.get_field(key, field)
        # Sort by the 'date' column in ascending order
        sorted_df = market_cap_df.sort(by="date")

        self.data_store.write_parquet(sorted_df, "processed", "marketcap")
        # Also add to cache to pick up later as its 'always' going to be
        self.data_cache["processed_marketcap"] = sorted_df