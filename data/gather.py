import click
from data.models.general import DataGatherer, DataStore
from _secrets import FMP_API_KEY
from data.models.prices import PricesDataHandler
from data.models.profile import ProfileDataHandler
from data.models.financial_statemenets import FinancialStatementsDataHandler
from data.models.market_cap import MarketCapDataHandler
from datetime import datetime as dt


@click.command()
@click.option('--no-refresh', is_flag=False, default=False, help='Skip data refresh and processing.')
@click.option('--fields', multiple=True, default=['adjClose', 'volume'],
              help='Fields to process (can specify multiple fields).')
@click.option('--engine', default='polars', help='Engine to use for reading/writing data (polars or pandas).')
@click.option('--folder', default='local_store', help='Folder where data files are stored.')
def refresh_data(fields, engine, folder, no_refresh):
    # Initialize DataHandler and DataStore

    # TODO: Now we have dt metadata, load data, check if more than 1 day then only refresh whats needed -have override too for refresh all

    data_store = DataStore(base_location='data/local_store', engine="polars")
    data_gatherer = DataGatherer(api_key=FMP_API_KEY, symbols=data_store.symbols, rate_limit=275,
                                 data_handler=data_store, max_retries=3)

    prices_data_handler = PricesDataHandler(data_gatherer, data_store, interval="historical-price-full",
                                            sub_directory="prices")
    market_cap_data_handler = MarketCapDataHandler(data_gatherer, data_store, start_date=dt(1990, 1, 1),
                                                   interval="historical-market-capitalization",
                                                   sub_directory="marketcap_v2")

    profiles_data_handler = ProfileDataHandler(data_gatherer, data_store)
    financial_statements_data_handler = FinancialStatementsDataHandler(data_gatherer, data_store,
                                                                       periods=['annual', 'quarter'])

    if not no_refresh:
        click.echo(f"Refreshing Data")
        prices_data_handler.update_data(prices_data_handler.build_url, prices_data_handler.process_raw_prices)
        # profiles_data_handler.update_data()
        # financial_statements_data_handler.update_data()
        # market_cap_data_handler.synchronously_backfill_market_caps()


if __name__ == '__main__':
    refresh_data()
