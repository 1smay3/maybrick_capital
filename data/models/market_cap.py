import asyncio
import polars as pl
from datetime import datetime, timedelta
from collections import defaultdict
import logging
from data.models.general import DataStore, DataGatherer


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class MarketCapDataHandler:
    def __init__(self, data_gatherer, data_store, interval, sub_directory, start_date):
        self.data_gatherer: DataGatherer = data_gatherer
        self.data_store: DataStore = data_store
        self.interval: str = interval
        self.api_key: str = data_gatherer.api_key
        self.endpoint_url: str = "https://financialmodelingprep.com/api/v3/{interval}/{symbol}?from={start_date}&to={end_date}&apikey={api_key}"
        self.sub_directory: str = sub_directory
        self.data_cache: defaultdict = defaultdict(pl.DataFrame)
        self.start_date: datetime = start_date

    def _shard_calls_into_date_chunks(
        self, start_date: datetime, chunk_size_days=450
    ) -> tuple[datetime, datetime]:
        """Chunk the data retrieval into sets of dates of a given size."""
        end_date = datetime.today()
        current_start_date = start_date
        date_chunks = []

        while current_start_date < end_date:
            current_end_date = min(
                current_start_date + timedelta(days=chunk_size_days), end_date
            )
            date_chunks.append(
                (
                    current_start_date.strftime("%Y-%m-%d"),
                    current_end_date.strftime("%Y-%m-%d"),
                )
            )
            current_start_date = current_end_date + timedelta(days=1)

        return date_chunks

    def build_url(self, symbol: str, date_chunk: tuple[datetime, datetime]) -> str:
        """Build the URL for fetching data for a specific date range."""
        return self.endpoint_url.format(
            interval=self.interval,
            symbol=symbol,
            start_date=date_chunk[0],
            end_date=date_chunk[1],
            api_key=self.api_key,
        )

    def _process_data(self, data: list) -> pl.DataFrame:
        return self.__process_raw_marketcap(data)

    def __process_raw_marketcap(self, data: list) -> pl.DataFrame:
        # TODO: Improve, quite unsafe
        if len(data) > 1:
            df = pl.DataFrame(data)
            df = df.select(pl.col("marketCap").cast(pl.Float64).alias("marketCap"))
        else:
            df = pl.DataFrame()

        return df

    async def gather_and_store_data(self):
        """Fetch data for all symbols and store it."""
        await self.data_gatherer._fetch_all_data(
            build_url=self.build_url,
            process_response=self._process_data,
            file_suffix=self.sub_directory,
            date_chunker=lambda symbol: self._shard_calls_into_date_chunks(
                self.start_date
            ),
        )

    def synchronously_backfill_market_caps(self):
        """Run the async gathering and storing process."""
        # Note: Ideally, and the initial idea, was that this would only refresh the last c.500 (due to api limit)
        # rows rather than refresh the whole history but for now, we put this in the backlog as it doesn't advance
        # the process rather it's just fast
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                raise RuntimeError(
                    "Cannot run 'update_data' while another event loop is running"
                )
            else:
                combined_results = loop.run_until_complete(self.gather_and_store_data())
                return combined_results
        except RuntimeError as e:
            print(f"RuntimeError: {e}")
            raise e

    def read_raw_data(self, sub_directory: str) -> dict:
        """Load raw data from the data store and cache it."""
        all_data = self.data_store.read_all_in_directory(sub_directory)
        self.data_cache[sub_directory] = all_data
        return all_data

    def _get_list_of_field_frames(self, key: str, field: str) -> list[pl.DataFrame]:
        """Fetch and process data for a specific field using cached data."""
        if key not in self.data_cache:
            raise ValueError(f"No data available for key: {key}")

        all_frames = self.data_cache[key]

        dfs = []
        for frame_name, frame_data in all_frames.items():
            symbol = frame_name.split("_")[1]
            data = frame_data[["date", field]].rename({field: symbol})
            dfs.append(data)
        return dfs

    def get_field(self, key: str, field: str) -> pl.DataFrame:
        """Get a merged DataFrame of a specific field across all symbols."""
        list_of_frames = self._get_list_of_field_frames(key, field)
        if not list_of_frames:
            return pl.DataFrame()  # Return an empty DataFrame if no frames available
        merged_df = list_of_frames[0]
        for df in list_of_frames[1:]:
            merged_df = merged_df.join(df, how="full", on="date", coalesce=True)
        no_duplicates_df = merged_df.unique(keep="first", subset="date")
        return no_duplicates_df

    def build_processed_market_caps(self, key: str) -> pl.DataFrame:
        if key not in self.data_cache:
            raise ValueError(f"No data available for key: {key}")

        field = "marketCap"  # Example field name; adjust as needed
        market_cap_df = self.get_field(key, field)
        sorted_df = market_cap_df.sort(by="date")
        self.data_store.write_parquet(
            sorted_df, "processed/market_data", "marketcap.parquet"
        )
        self.data_cache["processed_marketcap"] = sorted_df
