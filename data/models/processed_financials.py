import polars as pl
from collections import defaultdict
from tqdm import tqdm
from datetime import datetime as dt
class FinancialDataProcessor:
    def __init__(self, data_store, periods=['annual', 'quarter']):
        self.data_store = data_store
        self.periods = periods
        self.sub_directory = "financial_statements"
        self.data_cache = defaultdict(pl.DataFrame)
        self.ratios_to_process=  []

    def read_raw_data(self, sub_directory):
        """Load raw data from the data store and cache it."""
        all_data = self.data_store.read_all(sub_directory)
        # Cache data using sub_directory as key
        self.data_cache[sub_directory] = all_data
        return all_data

    def extract_ticker(self, base_path, string_to_replace):
        """Extract the ticker symbol from a given path."""
        parts = base_path.split('/')
        return parts[-1].replace(string_to_replace, '')

    def _add_metadata_to_statements(self, period):
        # Load financial statments
        statements = self.read_raw_data(f"financial_statements/{period}")
        # Load relevant SEC mapping
        sec_name = "10-K" if period=="annual" else "10-Q"
        sec_filings = self.read_raw_data(f"financial_statements/SEC/{sec_name}")


        for file_name in tqdm(statements.keys()):
            try:
                stock_symbol = self.extract_ticker(file_name, f"{period}_")

                financials = statements[file_name].with_columns(pl.col('date').str.strptime(pl.Date))
                date_mapper = sec_filings[f"financial_statements/SEC/{sec_name}_{stock_symbol}"].with_columns(
                    pl.col('fillingDate').str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S")
                )
                date_mapper = date_mapper.with_columns(
                    pl.col('fillingDate').dt.date().alias('fillingDate')
                )

                # Sort sec_filings by 'fillingDate' to facilitate efficient lookups
                date_mapper = date_mapper.sort('fillingDate')

                # Create an empty DataFrame to store the results
                # Perform a cross join and filter to get only future filingDates
                merged_df = financials.join(
                    date_mapper,
                    on='symbol',
                    how='left'
                ).filter(
                    pl.col('fillingDate') > pl.col('date')
                )

                # Group by 'date' and 'symbol' to get the smallest future filingDate
                result = merged_df.group_by(['date', 'symbol']).agg(
                    pl.col('fillingDate').min().alias('closest_filing_date')
                )

                # Merge the result back with financials
                final_df = financials.join(
                    result,
                    on=['date', 'symbol'],
                    how='left'
                )

                self.data_store.write_parquet(final_df, f"financial_statements/pre_processed/{period}", f"{stock_symbol}.parquet", log=False)
            except KeyError:
                print(f"Symbol: {stock_symbol} failed, check if it is missing in the statements or filings data, both are required")


    def _get_single_stock_field_daily(self, period, field):
        processed_financials = self.read_raw_data(
            f"financial_statements/pre_processed/{period}")


        field_data_store = []
        for stock, data in processed_financials.items():
            stock_symbol = self.extract_ticker(stock, f"{period}_")

            quarterly_data_only = data.filter(pl.col('documenttype') == "10-Q") #TODO: Handle better/ actually handle...

            if field in quarterly_data_only.columns:

                field_data = quarterly_data_only.select(["closest_filing_date", field])

                sorted_df = field_data.sort(by="closest_filing_date")

                start_date = field_data['closest_filing_date'].min()
                end_date = dt.today().date()
                date_range = pl.date_range(start=start_date, end=end_date, interval='1d')

                df_daily = pl.DataFrame(pl.date_range(
                    sorted_df['closest_filing_date'].min(),
                    dt.today().date(),
                    interval='1d',
                    eager=True
                ).alias('date')).join(
                    sorted_df,
                    left_on='date',
                    right_on = "closest_filing_date",
                    how='left'
                ).select([
                    pl.col('date'),
                    pl.all().exclude('date').forward_fill()
                ])

                # Rename col
                df_daily = df_daily.rename({
                    field: stock_symbol,
                })

                field_data_store.append(df_daily)

        merged_df = field_data_store[0]
        for df in field_data_store[1:]:
            merged_df = merged_df.join(df, on="date", how="outer", coalesce=True)

        return merged_df

    def _combine_single_stock_fields_daily(self):
        return None

