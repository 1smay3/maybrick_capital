import polars as pl
from collections import defaultdict

class AccountingRatioBuilder:
    def __init__(self, data_store, periods=['annual', 'quarter']):
        self.data_store = data_store
        self.periods = periods
        self.sub_directory = "processed/financials"
        self.data_cache = defaultdict(pl.DataFrame)
        self.ratios_to_process=  []

    def read_raw_data(self, sub_directory, period):
        """Load raw data from the data store and cache it."""
        all_data = self.data_store.read_all(f"{sub_directory}/{period}")
        # Cache data using sub_directory as key
        self.data_cache[sub_directory] = all_data
        return all_data

    def build_ratios(self):

        btp = self.data_cache['processed/financials']['processed/financials/quarterly_ShareholdersEquity'] / self.data_cache['']['processed/financials/quarterly_ShareholdersEquity']


        # book_price
        # sales_price
        # cf_price
        return None