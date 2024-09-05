from data.models.processed_financials import FinancialDataProcessor
import click
from data.models.general import DataGatherer, DataStore
from _secrets import FMP_API_KEY
from data.models.ratios import AccountingRatioBuilder
from data.models.prices import PricesDataHandler
from data.models.profile import ProfileDataHandler
from data.models.market_cap import MarketCapDataHandler
from datetime import datetime as dt

PRE_PROCESS_FINANCIAL_STATEMENTS = True



@click.command()
@click.option('--folder', default='local_store', help='Folder where data files are stored.')
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
    market_cap_data_handler = MarketCapDataHandler(data_gatherer, data_store, start_date=dt(1990,1, 1), interval="historical-market-capitalization", sub_directory="marketcap")
    # PROFILES ----------------------------------
    profiles_data_handler = ProfileDataHandler(data_gatherer, data_store)
    # FINANCIAL STATEMENTS ----------------------
    financial_statements_processor = FinancialDataProcessor(data_store)
    # RATIOS ------------------------------------
    ratios = AccountingRatioBuilder(data_store)

    # Run post-processing to get in format we want
    # prices_data_handler.read_raw_data("prices")
    # prices_data_handler.build_processed_prices("prices")
    # prices_data_handler.build_base_frame()
    #
    # profiles_data_handler.read_raw_data("profiles")
    # profiles_data_handler.combine_and_save_all_profiles()
    #
    market_cap_data_handler.read_raw_data("marketcap_v2")
    market_cap_data_handler.build_processed_market_caps("marketcap_v2")

    # KEY RATIOS ----------------------
    # Note: Some stocks dont have data for financials, so we start to drop columns here
    if PRE_PROCESS_FINANCIAL_STATEMENTS:
        pass
        # For the US, no semi-annual reporting
        # financial_statements_processor.add_metadata_to_statements("annual")
        # financial_statements_processor.add_metadata_to_statements("quarter")
        # financial_statements_processor.build_single_field_frames("quarter")

    # Load processed financial statements
    financial_statements_processor.standardise_data("processed", "financials", "quarter", "market_data")
    financial_statements_processor.post_process_financial_data()

    ratios.build_ratios()

if __name__ == '__main__':
    process_data()
