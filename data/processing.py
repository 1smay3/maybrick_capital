from data.models.processed_financials import FinancialDataProcessor
import click
from data.models.general import DataGatherer, DataStore
from _secrets import FMP_API_KEY
from data.models.ratios import AccountingRatioBuilder
from data.models.prices import PricesDataHandler
from data.models.profile import ProfileDataHandler


PRE_PROCESS_FINANCIAL_STATEMENTS = False



@click.command()
@click.option('--folder', default='local_store', help='Folder where data files are stored.') # TODO: Needs fixing to work if I move the folders easier, using root from os
@click.option('--engine', default='polars', help='Engine to use for reading/writing data (polars or pandas).')
def process_data(folder, engine):
    """Process data for a specific field and merge all symbol data."""

    # Initialize General DataHandlers
    data_store = DataStore(base_location='data/local_store', engine="polars")
    data_gatherer = DataGatherer(api_key=FMP_API_KEY, symbols=data_store.symbols, rate_limit=275, data_handler=data_store, max_retries=3)


    # Initialize Specific DataHandler
    # PRICES -------------------------------
    prices_data_handler = PricesDataHandler(data_gatherer, data_store, interval="historical-price-full", sub_directory="price")
    # MARKET CAP -------------------------------
    market_cap_data_handler = PricesDataHandler(data_gatherer, data_store, interval="historical-market-capitalization", sub_directory="marketcap")
    # PROFILES ----------------------------------
    profiles_data_handler = ProfileDataHandler(data_gatherer, data_store)
    # FINANCIAL STATEMENTS ----------------------
    financial_statements_processor = FinancialDataProcessor(data_store)
    # RATIOS ------------------------------------
    ratios = AccountingRatioBuilder(data_store)
    #
    # # Run post-processing to get in format we wan
    # prices_data_handler.read_raw_data("prices")
    # prices_data_handler.build_processed_prices("prices")
    #
    # profiles_data_handler.read_raw_data("profiles")
    # profiles_data_handler.combine_and_save_all_profiles()
    #
    # market_cap_data_handler.read_raw_data("marketcap")
    # market_cap_data_handler.build_processed_market_caps("marketcap")

    # KEY RATIOS ----------------------
    # Note: Some stocks dont have data for financials, so we start to drop columns here
    if PRE_PROCESS_FINANCIAL_STATEMENTS:
        # For the US, no semi-annual reporting
        financial_statements_processor.add_metadata_to_statements("annual")
        financial_statements_processor.add_metadata_to_statements("quarterly")
        financial_statements_processor.build_single_field_frames("quarterly")

    # Load processed financial statements
    financial_statements_processor.standardise_data("processed", "financials", "quarterly", "market_data")
    financial_statements_processor.post_process_financial_data()

    ratios.build_ratios()

if __name__ == '__main__':
    process_data()
