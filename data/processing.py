import click
from data.models.general import DataStore  # Assume this is the module where your DataHandler class is defined
from data.utils import pct_change
import click
from data.models.general import DataGatherer, DataStore
from _secrets import FMP_API_KEY
from data.models.prices import PricesDataHandler
from data.models.profile import ProfileDataHandler
@click.command()
@click.option('--folder', default='local_store', help='Folder where data files are stored.') # TODO: Needs fixing to work if I move the folders easier, using root from os
@click.option('--engine', default='polars', help='Engine to use for reading/writing data (polars or pandas).')

def process_data(folder, engine):
    """Process data for a specific field and merge all symbol data."""

    # Initialize DataHandler

    data_store = DataStore(base_location='data/local_store', engine="polars")
    data_gatherer = DataGatherer(api_key=FMP_API_KEY, symbols=data_store.symbols, rate_limit=275, data_handler=data_store, max_retries=3)

    prices_data_handler = PricesDataHandler(data_gatherer, data_store, interval="historical-price-full")
    profiles_data_handler = ProfileDataHandler(data_gatherer, data_store)

    # Run post-processing to get in format we use
    prices_data_handler.read_raw_data("prices")
    prices_data_handler.build_processed_prices("prices")


if __name__ == '__main__':
    process_data()
