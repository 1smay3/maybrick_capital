from data.utils import apply_schema
import asyncio
import polars as pl
from collections import defaultdict
class ProfileDataHandler:
    def __init__(self, data_gatherer, data_store):
        self.data_gatherer = data_gatherer
        self.data_store = data_store
        self.api_key = data_gatherer.api_key
        self.endpoint_url = 'https://financialmodelingprep.com/api/v3/{interval}/{symbol}?from=1900-01-01&apikey={api_key}'
        self.sub_directory = "profiles"  # Define the subdirectory for storing profile data
        self.data_cache = defaultdict(pl.DataFrame)  # To store and access data by key

    def build_url(self, symbol):
        """Build the URL for fetching data."""
        return self.endpoint_url.format(interval="profile", symbol=symbol, api_key=self.api_key)

    async def gather_and_store_data(self):
        """Fetch data for all symbols and store it."""
        await self.data_gatherer._fetch_all_data(self.build_url, self.process_response, self.sub_directory)

    def process_response(self, data):
        """Process the raw API response and convert it to a Polars DataFrame."""
        # Convert the JSON response to a Polars DataFrame
        return pl.DataFrame(data)

    def update_data(self):
        """Run the async gathering and storing process."""
        # Check if there's an existing event loop
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # Running in an existing event loop
                raise RuntimeError("Cannot run 'update_data' while another event loop is running")
            else:
                # Run in the event loop
                loop.run_until_complete(self.gather_and_store_data())
        except RuntimeError as e:
            print(f"RuntimeError: {e}")
            # If there's an existing event loop, we should handle it gracefully
            # Example: you might need to adjust your code to run this method in an async context
            raise e

    def read_raw_data(self, sub_directory):
        """Load raw data from the data store and cache it."""
        all_data = self.data_store.read_all(sub_directory)
        # Cache data using sub_directory as key
        self.data_cache[sub_directory] = all_data
        return all_data

    def combine_all_profiles(self):
        # Choose a frame to use as base schema:
        schema = self.data_cache["profiles"]["profiles_MCD"].schema

        for k,v in self.data_cache["profiles"].items():
            self.data_cache["profiles"][k] = apply_schema(v, schema)

        combined_frame = pl.concat(self.data_cache["profiles"].values())

        self.data_store.write_parquet(combined_frame, "processed", "all_profiles")
        # Also add to cache to pick up later
        self.data_cache["all_profiles"] = combined_frame

        return combined_frame