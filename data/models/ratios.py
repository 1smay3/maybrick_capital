import polars as pl
from collections import defaultdict
import pandas as pd


class AccountingRatioBuilder:
    def __init__(self, data_store, periods=['annual', 'quarter']):
        self.data_store = data_store
        self.periods = periods
        self.sub_directory = "processed/financials"
        self.data_cache = defaultdict(pl.DataFrame)
        self.ratios_to_process=  []

    def read_processed_financials(self, sub_directory, period):
        """Load raw data from the data store and cache it."""
        all_data = self.data_store.read_all(f"{sub_directory}/{period}")

        # Rename keys for easier use
        renamed_data_cache = {}
        for data_name, data in all_data.items():
            clean_name = data_name.replace(f"{sub_directory}/{period}_", "")
            renamed_data_cache[clean_name] = data

        self.data_cache["financials"] = renamed_data_cache

    def read_processed_market_data(self, sub_directory):
        all_data = self.data_store.read_all(f"{sub_directory}", parquet_suffix=False)
        DO_NOT_LOAD = ["all_profiles"]
        renamed_data_cache = {}
        for data_name, data in all_data.items():
            clean_name = data_name.split("\\")[-1]
            if clean_name in DO_NOT_LOAD:
                pass
            else:
                renamed_data_cache[clean_name] = data

        self.data_cache["market"] = renamed_data_cache

    def read_raw_data(self, market_data_sub_directory, financials_sub_directory, period):
        self.read_processed_financials(financials_sub_directory, period)
        self.read_processed_market_data(market_data_sub_directory)

    # TODO: Abstract the below to a method or similar in the data_store that normalises all frames?
    # TODO: dont do in such a rushed horrible way. Go back through and rework the datastore to make this easier.
    def _find_common_dates_and_columns(self, dataframes: list[pl.DataFrame]) -> tuple[pl.Series, list[str]]:
        all_dates = []
        all_columns = []
        for df in dataframes:
            # Force date column to no timestamp
            df_date = df.with_columns(pl.col("date").dt.date().alias("date"))
            # Get unique dates from the current DataFrame
            all_dates.extend(df_date["date"].to_list())
            all_columns.extend(df.columns)

        unique_dates = sorted(list(set(all_dates)))
        unique_columns = sorted(list(set(list([x for x in all_columns if x != "date"]))))
        return unique_dates, unique_columns#

    def _reindex_dataframes_to_base(self, dataframes: list[pl.DataFrame], common_dates: pl.Series,
                                    common_columns: list[str]) -> list[pl.DataFrame]:
        # Parse to pandas first
        blank_df = pd.DataFrame(columns = common_columns, index=common_dates)
        for df in dataframes:
            pandas_df = df.to_pandas().set_index("date")
            reindexed_df = pandas_df.reindex_like(blank_df)

    def build_ratios(self):
        # Build a list of all dfs
        # Combine the values from both 'market' and 'financials' into a single list
        combined_dataframes = list(self.data_cache['market'].values()) + list(self.data_cache['financials'].values())
        # Pass the combined list to the function
        unique_dates, unique_columns = self._find_common_dates_and_columns(combined_dataframes)

        reindexed_dataframes = self._reindex_dataframes_to_base(combined_dataframes, unique_dates, unique_columns)

        # Perform element-wise division of the same columns
        result = self.data_cache['market']['marketcap'].drop("date") / self.data_cache["financials"]['ShareholdersEquity'].drop("date")

        # Add back the date column if needed
        result = self.data_cache['market']['marketcap'].select("date").with_columns(result)


        # book_price
        # sales_price
        # cf_price
        return None