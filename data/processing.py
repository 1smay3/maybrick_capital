import click
from data.storage import DataHandler  # Assume this is the module where your DataHandler class is defined
from data.utils import pct_change

@click.command()
@click.option('--folder', default='local_store', help='Folder where data files are stored.') # TODO: Needs fixing to work if I move the folders easier, using root from os
@click.option('--engine', default='polars', help='Engine to use for reading/writing data (polars or pandas).')

def process_data(folder, engine):
    """Process data for a specific field and merge all symbol data."""

    # Initialize DataHandler
    data_handler = DataHandler(folder_name=folder, engine=engine)

    data_handler.read_all()  # Read all into memory following update

    # Total Returns
    prices = data_handler.read_parquet("adjClose")
    total_returns = pct_change(prices, 1)
    data_handler.write_parquet(total_returns,"total_return")

if __name__ == '__main__':
    process_data()
