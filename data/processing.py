from data.models.processed_financials import FinancialDataProcessor
import click
from data.models.general import DataGatherer, DataStore
from _secrets import FMP_API_KEY
from data.models.ratios import AccountingRatioBuilder
from data.models.prices import PricesDataHandler

PRE_PROCESS_FINANCIAL_STATEMENTS = False



@click.command()
@click.option('--folder', default='local_store', help='Folder where data files are stored.') # TODO: Needs fixing to work if I move the folders easier, using root from os
@click.option('--engine', default='polars', help='Engine to use for reading/writing data (polars or pandas).')
def process_data(folder, engine):
    """Process data for a specific field and merge all symbol data."""

    # Initialize DataHandler
    data_store = DataStore(base_location='data/local_store', engine="polars")
    data_gatherer = DataGatherer(api_key=FMP_API_KEY, symbols=data_store.symbols, rate_limit=275, data_handler=data_store, max_retries=3)

    # PRICES -------------------------------
    # prices_data_handler = PricesDataHandler(data_gatherer, data_store, interval="historical-price-full", sub_directory="price")
    # profiles_data_handler = ProfileDataHandler(data_gatherer, data_store)
    #
    # # Run post-processing to get in format we use
    # prices_data_handler.read_raw_data("prices")
    # prices_data_handler.build_processed_prices("prices") # TODO: change to save as .paequet
    #
    # # PROFILES -------------------------------
    # profiles_data_handler.read_raw_data("profiles")
    # profiles_data_handler.combine_all_profiles()

    # MARKET CAP -------------------------------
    # market_cap_data_handler = PricesDataHandler(data_gatherer, data_store, interval="historical-market-capitalization", sub_directory="marketcap") # Note: sub_directory must be single word

    # # Run post-processing to get in format we use
    # market_cap_data_handler.read_raw_data("marketcap")
    # market_cap_data_handler.build_processed_market_caps("marketcap")

    # KEY RATIOS
    financial_statements_processor = FinancialDataProcessor(data_store)
    # Note: here is where we start to drop data - some stocks dont have data for financials
    if PRE_PROCESS_FINANCIAL_STATEMENTS:
        financial_statements_processor._add_metadata_to_statements("annual")
        financial_statements_processor._add_metadata_to_statements("quarter")

    # Load processed financial statemnts
    # processed_quarterlys = financial_statements_processor.process_fields_to_dictionary("quarter", ["revenuefromcontractwithcustomerexcludingassessedtax", "stockholdersequity", "netcashprovidedbyusedinoperatingactivities", "weightedaveragenumberofdilutedsharesoutstanding"], save_name="quarterly_financials")
    ratios = AccountingRatioBuilder(data_store)
    ratios.read_raw_data("processed", "processed/financials", "quarterly") # TODO: fix saving and loading rather than patching loading funcs

    ratios.build_ratios()

if __name__ == '__main__':
    process_data()
