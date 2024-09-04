import asyncio
import polars as pl
from collections import defaultdict


class FinancialStatementsDataHandler:
    def __init__(self, data_gatherer, data_store, periods):
        self.data_gatherer = data_gatherer
        self.data_store = data_store
        self.periods = periods
        self.api_key = data_gatherer.api_key
        self.base_financials_url = "https://financialmodelingprep.com/api/v3/financial-statement-full-as-reported/{symbol}?period={period}&apikey={api_key}"
        self.base_sec_url = "https://financialmodelingprep.com/api/v3/sec_filings/{symbol}?type={type}&page=0&apikey={api_key}"
        self.sub_directory = "financial_statements"
        self.data_cache = defaultdict(pl.DataFrame)  # To store and access data by key
        self.sec_data_cache = defaultdict(dict)  # Cache for SEC filings

    def build_financials_url(self, symbol, period):
        """Build the URL for fetching financial statements data."""
        return self.base_financials_url.format(
            symbol=symbol, period=period, api_key=self.api_key
        )

    def build_sec_url(self, symbol, sec_type):
        """Build the URL for fetching SEC filings data."""
        return self.base_sec_url.format(
            symbol=symbol, type=sec_type, api_key=self.api_key
        )

    def build_url(self, symbol, period):
        """Build the URL for fetching financial data for a specific symbol and period."""
        return self.build_financials_url(symbol, period)

    async def gather_and_store_data(self):
        """Fetch data for all symbols and store it."""
        # Fetch financial statements data
        for period in self.periods:
            # Use the build_url method instead of a lambda
            await self.data_gatherer._fetch_all_data(
                lambda symbol: self.build_financials_url(symbol, period),
                self._process_financial_data,
                f"{self.sub_directory}/{period}",
            )

        # Fetch SEC filings data
        for sec_type in ["10-K", "10-Q"]:
            for sec_type in ["10-K", "10-Q"]:
                await self.data_gatherer._fetch_all_data(
                    lambda symbol: self.build_sec_url(symbol, sec_type),
                    self._process_sec_data,
                    f"{self.sub_directory}/SEC/{sec_type}",
                )

    def _process_financial_data(self, data):
        """Process the financial data into a DataFrame."""
        df = pl.DataFrame(data)
        return df

    def _process_sec_data(self, data):
        """Process the SEC filings data into a DataFrame."""
        df = pl.DataFrame(data)
        return df

    def update_data(self):
        """Run the async gathering and storing process for all symbols."""
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                raise RuntimeError(
                    "Cannot run 'update_data' while another event loop is running"
                )
            else:
                loop.run_until_complete(self.gather_and_store_data())
        except RuntimeError as e:
            print(f"RuntimeError: {e}")
            raise e

    def read_raw_data(self, symbol, period):
        """Load raw data for a specific symbol and period from the data store and cache it."""
        sub_directory = f"{self.sub_directory}/{symbol}/{period}"
        all_data = self.data_store.real_all_in_directory(sub_directory)
        self.data_cache[sub_directory] = all_data
        return all_data

    def _get_list_of_field_frames(self, key, field):
        """Fetch and process data for a specific field using cached data."""
        if key not in self.data_cache:
            raise ValueError(f"No data available for key: {key}")

        all_frames = self.data_cache[key]
        dfs = []

        for frame_name, frame_data in all_frames.items():
            symbol = frame_name.split("/")[2]
            data = frame_data[["date", field]].rename({field: symbol})
            dfs.append(data)

        return dfs

    def get_field(self, symbol, field, period):
        """Get a merged DataFrame of a specific field across all periods for a symbol."""
        key = f"{self.sub_directory}/{symbol}/{period}"
        list_of_frames = self._get_list_of_field_frames(key, field)
        if not list_of_frames:
            return pl.DataFrame()  # Return an empty DataFrame if no frames available

        merged_df = list_of_frames[0]
        for df in list_of_frames[1:]:
            merged_df = merged_df.join(df, on="date", how="outer", coalesce=True)

        return merged_df
