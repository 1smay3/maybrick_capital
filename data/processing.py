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
    # prices_data_handler = PricesDataHandler(data_gatherer, data_store, interval="historical-price-full", sub_directory="rice")
    # profiles_data_handler = ProfileDataHandler(data_gatherer, data_store)
    #
    # # Run post-processing to get in format we use
    # prices_data_handler.read_raw_data("prices")
    # prices_data_handler.build_processed_prices("prices")
    #
    # # PROFILES -------------------------------
    # profiles_data_handler.read_raw_data("profiles")
    # profiles_data_handler.combine_all_profiles()

    # MARKET CAP -------------------------------
    market_cap_data_handler = PricesDataHandler(data_gatherer, data_store, interval="historical-market-capitalization", sub_directory="market_cap")

    # # Run post-processing to get in format we use
    market_cap_data_handler.read_raw_data("market_cap")
    market_cap_data_handler.build_processed_prices("market_cap")

    # KEY RATIOS
    financial_statements_processor = FinancialDataProcessor(data_store)
    # Note: here is where we start to drop data - some stocks dont have data for financials
    if PRE_PROCESS_FINANCIAL_STATEMENTS:
        financial_statements_processor._add_metadata_to_statements("annual")
        financial_statements_processor._add_metadata_to_statements("quarter")

    # Load processed financial statemnts
    processed_quarterlys = financial_statements_processor.process_fields_to_dictionary("quarter", ["revenuefromcontractwithcustomerexcludingassessedtax", "stockholdersequity", "netcashprovidedbyusedinoperatingactivities", "weightedaveragenumberofdilutedsharesolutstanding"], save_name="quarterly_financials")
    ratios = AccountingRatioBuilder(data_store)
    ratios.read_raw_data("processed/financials", "quarterly")
    ratios.read_raw_data("processed", "prices")
    ratios.read_raw_data("processed", "market_cap")

    ratios.build_ratios()

if __name__ == '__main__':
    process_data()
