import polars as pl
import asyncio
from data.models.general import DataGatherer, DataStore

class ProfileDataHandler:
    def __init__(self, data_gatherer, data_store):
        self.data_gatherer = data_gatherer
        self.data_store = data_store
        self.api_key = data_gatherer.api_key
        self.endpoint_url = 'https://financialmodelingprep.com/api/v3/{interval}/{symbol}?from=1900-01-01&apikey={api_key}'
        self.sub_directory = "profiles"  # Define the subdirectory for storing profile data

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
