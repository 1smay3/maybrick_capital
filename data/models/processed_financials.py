import polars as pl
from collections import defaultdict
from tqdm import tqdm
from datetime import datetime as dt
import os
import pandas as pd

data_field_map = {
    "revenuefromcontractwithcustomerexcludingassessedtax": "Revenue_1",
    "revenues": "Revenue_2",  # Note: Some firms its marked against revenue, e.g AIG #https://www.sec.gov/ix?doc=/Archives/edgar/data/5272/000000527224000079/aig-20240630.htm
    "stockholdersequity": "ShareholdersEquity",
    "netcashprovidedbyusedinoperatingactivities": "OperatingCashFlow",
    "weightedaveragenumberofdilutedsharesoutstanding": "DilutedNOS",
}

TTM_FIELDS = [
    "revenuefromcontractwithcustomerexcludingassessedtax",
    "netcashprovidedbyusedinoperatingactivities",
]


class FinancialDataProcessor:
    def __init__(self, data_store, periods=["annual", "quarterly"]):
        self.data_store = data_store
        self.periods = periods
        self.sub_directory = "financial_statements"
        self.data_cache = defaultdict(pl.DataFrame)
        self.ratios_to_process = []

    def read_raw_data(self, sub_directory):
        """Load raw data from the data store and cache it."""
        all_data = self.data_store.real_all_in_directory(sub_directory)
        # Cache data using sub_directory as key
        self.data_cache[sub_directory] = all_data
        return all_data

    def extract_ticker(self, base_path, string_to_replace):
        """Extract the ticker symbol from a given path."""
        parts = base_path.split("/")
        return parts[-1].replace(string_to_replace, "")

    def add_metadata_to_statements(self, period):
        # Load financial statments
        statements = self.read_raw_data(f"financial_statements/{period}")
        # Load relevant SEC mapping
        sec_name = "10-K" if period == "annual" else "10-Q"
        sec_filings = self.read_raw_data(f"financial_statements/SEC/{sec_name}")

        for file_name in tqdm(statements.keys()):
            try:
                stock_symbol = self.extract_ticker(file_name, f"{period}_")

                financials = statements[file_name].with_columns(
                    pl.col("date").str.strptime(pl.Date)
                )
                date_mapper = sec_filings[
                    f"financial_statements/SEC/{sec_name}_{stock_symbol}"
                ].with_columns(
                    pl.col("fillingDate").str.strptime(
                        pl.Datetime, format="%Y-%m-%d %H:%M:%S"
                    )
                )
                date_mapper = date_mapper.with_columns(
                    pl.col("fillingDate").dt.date().alias("fillingDate")
                )

                # Sort sec_filings by 'fillingDate' to facilitate efficient lookups
                date_mapper = date_mapper.sort("fillingDate")

                # Create an empty DataFrame to store the results
                # Perform a cross join and filter to get only future filingDates
                merged_df = financials.join(
                    date_mapper, on="symbol", how="left"
                ).filter(pl.col("fillingDate") > pl.col("date"))

                # Group by 'date' and 'symbol' to get the smallest future filingDate
                result = merged_df.group_by(["date", "symbol"]).agg(
                    pl.col("fillingDate").min().alias("closest_filing_date")
                )

                # Merge the result back with financials
                final_df = financials.join(result, on=["date", "symbol"], how="left")

                self.data_store.write_parquet(
                    final_df,
                    f"financial_statements/pre_processed/{period}",
                    f"{stock_symbol}.parquet",
                    log=False,
                )
            except KeyError:
                print(
                    f"Symbol: {stock_symbol} failed, check if it is missing in the statements or filings data, both are required"
                )

    # Fields are mapped here
    # https: // www.sec.gov / ix?doc = / Archives / edgar / data / 1018724 / 000101
    # 872424000130 / amzn - 20240630.
    # htm

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

    def read_all_data_to_clean(
        self, processed_dir, financials_dir, period, markets_dir
    ):
        # TODO: rename funcs and remove rundendancy...
        financials_processed_data = os.path.join(processed_dir, financials_dir, period)
        market_processed_data = os.path.join(processed_dir, markets_dir)
        self.read_processed_financials(financials_processed_data)
        self.read_processed_market_data(market_processed_data)

    def _get_single_stock_field_daily(self, period, field):
        processed_financials = self.read_raw_data(
            f"financial_statements/pre_processed/{period}"
        )

        field = field.lower()

        field_data_store = []
        for stock, data in processed_financials.items():
            stock_symbol = self.extract_ticker(stock, f"{period}_")

            quarterly_data_only = data.filter(
                pl.col("documenttype") == "10-Q"
            )  # TODO: Handle better/ actually handle...

            if field in quarterly_data_only.columns:
                field_data = quarterly_data_only.select(["closest_filing_date", field])

                sorted_df = field_data.sort(by="closest_filing_date")

                # Apply TTM if we need/want it
                if field in TTM_FIELDS and period == "quarterly":
                    sorted_df = sorted_df.with_columns(
                        pl.col(field)
                        .rolling_sum(window_size=4, min_periods=4)
                        .alias(field)
                    )

                # TODO: get rid of this dependancy, build ourselves from prices
                business_days = pd.date_range(
                    start=sorted_df["closest_filing_date"].min(),
                    end=dt.today().date(),
                    freq="B",
                )

                df_business_days = pl.DataFrame({"date": business_days})
                df_business_days = df_business_days.with_columns(
                    pl.col("date").cast(pl.Date)
                )

                df_daily = df_business_days.join(
                    sorted_df,
                    left_on="date",
                    right_on="closest_filing_date",
                    how="left",
                ).select([pl.col("date"), pl.all().exclude("date").forward_fill()])

                # Rename col
                df_daily = df_daily.rename(
                    {
                        field: stock_symbol,
                    }
                )

                field_data_store.append(df_daily)

        merged_df = field_data_store[0]
        for df in field_data_store[1:]:
            merged_df = merged_df.join(df, on="date", how="outer", coalesce=True)

        # DILUTED NOS:#                      A          AAL  ...        ZBRA          ZTS
        # date                                  ...
        # 2015-02-27  338000000.0  737100000.0  ...  51251000.0  501610000.0
        # 2023-07-26  297000000.0  718890000.0  ...  51748069.0  464600000.0
        # 2024-07-25  294000000.0  720712000.0  ...  51790501.0  458800000.0
        # 2024-07-25  294000000.0  720712000.0  ...  51790501.0  458800000.0
        # 2024-07-25  294000000.0  720712000.0  ...  51790501.0  458800000.0
        # 2024-07-25  294000000.0  720712000.0  ...  51790501.0  458800000.0
        # 2024-07-25  294000000.0  720712000.0  ...  51790501.0  458800000.0

        no_duplicates_df = merged_df.unique(
            keep="first", subset="date"
        )  # TODO: [MAYCAP-8] This isn't causing any issues as the data checks out,
        # but valuable to know why and how the duplicates are being introduced

        return no_duplicates_df.sort(by="date")

    def build_single_field_frames(self, period):
        processed_data = {}
        for field in data_field_map.keys():
            processed_data[field] = self._get_single_stock_field_daily(period, field)

        for data_name, data_data in processed_data.items():
            data_nice_name = data_field_map[data_name]
            self.data_store.write_parquet(
                data_data,
                f"processed/financials/{period}",
                f"{data_nice_name}.parquet",
                log=True,
            )
        return processed_data

    def _find_common_dates_and_columns(
        self, dataframes: list[pl.DataFrame]
    ) -> tuple[pl.Series, list[str]]:
        all_dates = []
        all_columns = []
        for df in dataframes:
            # Force date column to no timestamp
            df_date = df.with_columns(pl.col("date").dt.date().alias("date"))
            # Get unique dates from the current DataFrame
            all_dates.extend(df_date["date"].to_list())
            all_columns.extend(df.columns)

        unique_dates = sorted(list(set(all_dates)))
        unique_columns = sorted(
            list(set(list([x for x in all_columns if x != "date"])))
        )
        return unique_dates, unique_columns  #

    def _reindex_dataframes_to_base(
        self, dataframes_dict, common_dates: pl.Series, common_columns: list[str]
    ) -> list[pl.DataFrame]:
        # Parse to pandas first
        base_frame = (
            self.data_store.read_parquet("core_data", "base_frame.parquet")
            .to_pandas()
            .set_index("date")
        )
        reindexed_frames = {}
        for dataframe_name, dataframe in dataframes_dict.items():
            pandas_df = dataframe.to_pandas().set_index("date")
            reindexed_df = pl.from_pandas(
                pandas_df.reindex_like(base_frame).reset_index()
            )
            reindexed_frames[dataframe_name] = reindexed_df

        return reindexed_frames

    def standardise_data(self, processed_dir, financials_dir, period, markets_dir):
        self.read_all_data_to_clean(processed_dir, financials_dir, period, markets_dir)

        if "financials" and "market" in self.data_cache.keys():
            # Build a list of all dfs
            # Combine the values from both 'market' and 'financials' into a single list
            combined_dataframes = list(self.data_cache["market"].values()) + list(
                self.data_cache["financials"].values()
            )
            # Pass the combined list to the function
            unique_dates, unique_columns = self._find_common_dates_and_columns(
                combined_dataframes
            )

            combined_dataframes_dict = {
                **self.data_cache["market"],
                **self.data_cache["financials"],
            }

            financial_data_dict = self._reindex_dataframes_to_base(
                combined_dataframes_dict, unique_dates, unique_columns
            )

            for data_name, data in financial_data_dict.items():
                # Now just save into local store, to make loading ez
                self.data_store.write_parquet(
                    data, "core_data", f"{data_name}.parquet", log=True
                )

    def post_process_financial_data(self):
        # Combine Two Types of Revenue into one due to weird GAAP namings
        revenue_1 = self.data_store.read_parquet("core_data", "Revenue_1.parquet")
        revenue_2 = self.data_store.read_parquet("core_data", "Revenue_2.parquet")

        # Fill nulls in df1 with values from df2 for all columns
        combined_revenue = revenue_1.with_columns(
            [pl.col(col).fill_null(revenue_2[col]) for col in revenue_1.columns]
        )

        # TODO: Figure out who the nulls are and why
        # df_with_null_count = combined_revenue.with_columns(
        #     pl.fold(
        #         acc=pl.lit(0),
        #         function=lambda acc, x: acc + x.is_null().cast(pl.Int32),
        #         exprs=pl.col("*")
        #     ).alias("null_count")
        # )

        self.data_store.write_parquet(
            combined_revenue, "core_data", "revenue.parquet", log=True
        )

        return None
