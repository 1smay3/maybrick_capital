import click
from data.storage import DataHandler  # Assume this is the module where your DataHandler class is defined
import polars as pl
from _secrets import FMP_API_KEY
from data.models.prices import PricesDataHandler


@click.group()
def cli():
    """CLI for managing and processing financial data."""
    pass


@click.command()
@click.option('--engine', default='polars', help='Engine to use for reading/writing data (polars or pandas).')
@click.option('--folder', default='local_database', help='Folder where data files are stored.')
def update_data(engine, folder):
    """Update and cache all S&P 500 data."""
    click.echo(f"Initializing DataHandler with engine={engine} and folder={folder}...")

    # Initialize DataHandler
    data_handler = DataHandler(folder_name=folder, engine=engine)

    # The data_handler will automatically fetch symbols and read all data on initialization
    click.echo("Data updated and cached successfully.")


@click.command()
@click.option('--no-refresh', is_flag=True, default=True, help='Skip data refresh and processing.')
@click.option('--fields', multiple=True, default=['adjClose', 'volume'], help='Fields to process (can specify multiple fields).')
@click.option('--engine', default='polars', help='Engine to use for reading/writing data (polars or pandas).')
@click.option('--folder', default='local_store', help='Folder where data files are stored.') # TODO: Needs fixing to work if I move the folders easier, using root from os

def refresh_data(fields, engine, folder, no_refresh):
    """Process data for a specific field and merge all symbol data."""

    # Initialize DataHandler
    data_handler = DataHandler(folder_name=folder, engine=engine)

    # Initialise StockDataHandler
    stock_data_handler = PricesDataHandler(api_key=FMP_API_KEY, symbols=data_handler.symbols, interval="historical-price-full",
                                           rate_limit=275, data_handler=data_handler)

    if not no_refresh:
        click.echo(f"Skipping Data Refresh!")
        stock_data_handler.update_data()

    data_handler.read_all()  # Read all into memory following update

    for field in fields:
        click.echo(f"Processing data for field={field}")
        data = data_handler.get_field(field)
        data_handler.write_parquet(data, field)


if __name__ == '__main__':
    refresh_data()
