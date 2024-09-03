import asyncio
import polars as pl
from collections import defaultdict
from constants import DATA_START_DATE
from datetime import datetime, timedelta

class MarketCapDataHandler:
    # TODO: make a synchronous version that backfills the library in chunks of 500
    def __init__(self, data_gatherer, data_store, interval, sub_directory, start_date=DATA_START_DATE):
        self.data_gatherer = data_gatherer
        self.data_store = data_store
        self.interval = interval
        self.api_key = data_gatherer.api_key
        self.endpoint_url = 'https://financialmodelingprep.com/api/v3/{interval}/{symbol}?from={start_date}&to={end_date}&apikey={api_key}'
        self.sub_directory = sub_directory  # Define the subdirectory for storing market cap data
        self.data_cache = defaultdict(pl.DataFrame)  # To store and access data by key
        self.start_date = start_date

    def _shard_calls_into_date_chunks(self, start_date, chunk_size_days=450):
        """Chunk the data retrieval into sets of dates of a given size."""
        end_date = datetime.today()
        current_start_date = start_date
        date_chunks = []

        while current_start_date < end_date:
            current_end_date = min(current_start_date + timedelta(days=chunk_size_days), end_date)
            date_chunks.append((current_start_date.strftime('%Y-%m-%d'), current_end_date.strftime('%Y-%m-%d')))
            current_start_date = current_end_date + timedelta(days=1)

        return date_chunks

    def synchronously_backfill_market_caps(self):
        raise NotImplementedError

    def build_url(self, symbol, start_date, end_date):
        """Build the URL for fetching data for a specific date range."""
        return self.endpoint_url.format(interval=self.interval, symbol=symbol, start_date=start_date, end_date=end_date, api_key=self.api_key)

    async def gather_and_store_data(self):
        """Fetch data for all symbols and store it."""
        date_chunks = self._shard_calls_into_date_chunks(self.start_date)


        for start_date, end_date in date_chunks:
            build_url = lambda symbol: self.build_url(symbol, start_date, end_date)
            await self.data_gatherer._fetch_all_data(build_url, self._process_data, f"{self.sub_directory}")


    def __process_raw_marketcap(self, data):
        df = pl.DataFrame(data)
        df = df.with_columns(pl.col('date').str.strptime(pl.Datetime))
        return df

    def _process_data(self, data):
        response = self.__process_raw_marketcap(data)
        return response

    def _save_data(self, symbol, data_frame):
        """Save the concatenated DataFrame for a symbol."""
        file_name = f"{symbol}_marketcap.parquet"
        self.data_store.write_parquet(data_frame, "processed/market_data", file_name)

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
        all_data = self.data_store.read_all_in_directory(sub_directory)
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
            merged_df = merged_df.join(df, how="full", on="date", coalesce=True)
        no_duplicates_df = merged_df.unique(keep="first", subset="date")
        return no_duplicates_df

    def build_processed_market_caps(self, key):
        if key not in self.data_cache:
            raise ValueError(f"No data available for key: {key}")

        field = "marketCap"  # Example field name; adjust as needed
        market_cap_df = self.get_field(key, field)
        # Sort by the 'date' column in ascending order
        sorted_df = market_cap_df.sort(by="date")

        self.data_store.write_parquet(sorted_df, "processed/market_data", "marketcap.parquet")
        # Also add to cache to pick up later as it's 'always' going to be
        self.data_cache["processed_marketcap"] = sorted_df
