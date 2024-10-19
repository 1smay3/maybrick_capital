import polars as pl
from data.models.general import GenericDataHandler
import logging
import asyncio


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class MarketCapDataHandler(GenericDataHandler):
    def __init__(self, data_gatherer, data_store, interval, sub_directory, start_date):
        super().__init__(data_gatherer, data_store, sub_directory)
        self.interval = interval
        self.api_key = data_gatherer.api_key
        self.endpoint_url = (
            "https://financialmodelingprep.com/api/v3/{interval}/{symbol}?from={start_date}"
            "&to={end_date}&apikey={api_key}"
        )
        self.start_date = start_date
    # TODO: Make it check for the data, load, check lasat date, and only get new chunks (make this method general - Append method)
    def build_url(self, symbol, date_chunk):
        """Build the URL for fetching data."""
        return self.endpoint_url.format(
            interval=self.interval,
            symbol=symbol,
            start_date=date_chunk[0],
            end_date=date_chunk[1],
            api_key=self.api_key,
        )

    def _process_data(self, data):
        """Process the raw market cap data."""
        return self.__process_raw_marketcap(data)

    def __process_raw_marketcap(self, data):
        """Convert raw data into a Polars DataFrame."""
        if len(data) > 1:
            df = pl.DataFrame(data)
            df = df.select(
                pl.col("marketCap").cast(pl.Float64).alias("marketCap"),
                pl.col("date"),
            )
        else:
            df = pl.DataFrame()
        return df

    def synchronously_backfill_market_caps(self):
        """Run the async gathering and storing process."""
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                raise RuntimeError("Cannot run 'update_data' while another event loop is running")
            else:
                combined_results = loop.run_until_complete(self.gather_and_store_data())
                return combined_results
        except RuntimeError as e:
            print(f"RuntimeError: {e}")
            raise e

    def build_processed_market_caps(self, key):
        """Build and store processed market cap data."""
        if key not in self.data_cache:
            raise ValueError(f"No data available for key: {key}")

        field = "marketCap"
        market_cap_df = self.get_field(key, field)
        market_cap_df = market_cap_df.with_columns(pl.col("date").str.strptime(pl.Datetime))
        sorted_df = market_cap_df.sort(by="date")

        self.data_store.write_parquet(sorted_df, "processed/market_data", "marketcap.parquet")
        self.data_cache["processed_marketcap"] = sorted_df
