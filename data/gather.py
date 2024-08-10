import click
from data.models.general import DataGatherer, DataStore
from _secrets import FMP_API_KEY
from data.models.prices import PricesDataHandler
from data.models.profile import ProfileDataHandler

@click.command()
@click.option('--no-refresh', is_flag=True, default=True, help='Skip data refresh and processing.')
@click.option('--fields', multiple=True, default=['adjClose', 'volume'], help='Fields to process (can specify multiple fields).')
@click.option('--engine', default='polars', help='Engine to use for reading/writing data (polars or pandas).')
@click.option('--folder', default='local_store', help='Folder where data files are stored.') # TODO: Needs fixing to work if I move the folders easier, using root from os
def refresh_data(fields, engine, folder, no_refresh):


    # Initialize DataHandler and DataStore
    data_store = DataStore(base_location='data/local_store', engine="polars")
    data_gatherer = DataGatherer(api_key=FMP_API_KEY, symbols=data_store.symbols, rate_limit=275, data_handler=data_store, max_retries=3)

    prices_data_handler = PricesDataHandler(data_gatherer, data_store, interval="historical-price-full")
    profiles_data_handler = ProfileDataHandler(data_gatherer, data_store)

    if not no_refresh:
        click.echo(f"Refreshing Data")
        prices_data_handler.update_data()
        profiles_data_handler.update_data()


if __name__ == '__main__':
    refresh_data()
