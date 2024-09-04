import polars as pl
from collections import defaultdict


class AccountingRatioBuilder:
    def __init__(self, data_store, periods=["annual", "quarterly"]):
        self.data_store = data_store
        self.periods = periods
        self.sub_directory = "processed/financials"
        self.data_cache = defaultdict(pl.DataFrame)
        self.ratios_to_process = []

    def read_processed_financials(self, directory):
        """Load raw data from the data store and cache it."""
        all_data = self.data_store.real_all_in_directory(directory)

        # Rename keys for easier use
        renamed_data_cache = {}
        for data_name, data in all_data.items():
            clean_name = data_name.replace(f"{directory}_", "")
            renamed_data_cache[clean_name] = data

        self.data_cache["financials"] = renamed_data_cache

    def read_processed_market_data(self, sub_directory):
        all_data = self.data_store.real_all_in_directory(
            f"{sub_directory}", parquet_suffix=False
        )
        DO_NOT_LOAD = ["all_profiles"]
        renamed_data_cache = {}
        for data_name, data in all_data.items():
            clean_name = data_name.split("\\")[-1].split(".")[0]
            if clean_name in DO_NOT_LOAD:
                pass
            else:
                renamed_data_cache[clean_name] = data

        self.data_cache["market"] = renamed_data_cache

    def build_ratios(self):
        # Load the required data
        financial_data_dict = self.data_store.read_core_data()
        # PRICE-TO-BOOK
        ptb = financial_data_dict["marketcap"].drop("date") / financial_data_dict[
            "ShareholdersEquity"
        ].drop("date")
        ptb = financial_data_dict["marketcap"].select("date").with_columns(ptb)
        self.data_store.write_parquet(ptb, "core_data", "ptb.parquet", log=True)

        # SALES-TO-PRICE
        stp = financial_data_dict["revenue"].drop("date") / financial_data_dict[
            "marketcap"
        ].drop("date")
        stp = financial_data_dict["revenue"].select("date").with_columns(stp)
        self.data_store.write_parquet(stp, "core_data", "stp.parquet", log=True)

        # CASH FLOW-TO_PRICE
        cftp = financial_data_dict["OperatingCashFlow"].drop(
            "date"
        ) / financial_data_dict["marketcap"].drop("date")
        cftp = financial_data_dict["OperatingCashFlow"].select("date").with_columns(stp)
        self.data_store.write_parquet(cftp, "core_data", "cftp.parquet", log=True)

        return None
