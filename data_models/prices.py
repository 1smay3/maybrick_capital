import aiohttp
import asyncio
import polars as pl
from constants import FLOAT_FIELDS_PRICES
import logging
import time


"""
Note: This end point also includes volumes, its broadly all the primary data we need
"""


# Set up basic configuration for logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class PricesDataHandler:
    def __init__(self, api_key, symbols, interval='1min', rate_limit=300, data_handler=None, max_retries=3):
        self.api_key = api_key
        self.symbols = symbols
        self.interval = interval
        self.rate_limit = rate_limit
        self.semaphore = asyncio.Semaphore(rate_limit)
        self.data_handler = data_handler or DataHandler()
        self.max_retries = max_retries

    async def _fetch_stock_data(self, session, symbol):
        url = f'https://financialmodelingprep.com/api/v3/{self.interval}/{symbol}?from=1900-01-01&apikey={self.api_key}'
        attempt = 0

        while attempt < self.max_retries:
            async with self.semaphore:
                logging.info(f'Starting to fetch data for symbol: {symbol} (Attempt {attempt + 1})')
                try:
                    async with session.get(url, ssl=False) as response:
                        if response.status == 200:
                            data = await response.json()

                            # Convert fields to float
                            for record in data['historical']:
                                for field in FLOAT_FIELDS_PRICES:
                                    record[field] = float(record.get(field, 0))

                            # Create and return Polars DataFrame
                            df = pl.DataFrame(data['historical'])
                            # Ensure correct date parsing
                            df = df.with_columns(pl.col('date').str.strptime(pl.Datetime))
                            logging.info(f'Fetched data for symbol: {symbol}')
                            return symbol, df

                        elif response.status == 429:  # Rate limit exceeded
                            wait_time = int(response.headers.get('Retry-After',
                                                                 60))  # Get Retry-After header or default to 60 seconds
                            logging.warning(
                                f'Rate limit exceeded for symbol: {symbol}. Waiting for {wait_time} seconds.')
                            await asyncio.sleep(wait_time)
                        else:
                            response.raise_for_status()

                except aiohttp.ClientResponseError as e:
                    logging.error(f'Client response error for symbol: {symbol}. Error: {e}')
                except aiohttp.ClientError as e:
                    logging.error(f'Client error for symbol: {symbol}. Error: {e}')

                attempt += 1

        # If we exhausted all attempts and still failed
        logging.error(f'Failed to fetch data for symbol: {symbol} after {self.max_retries} attempts.')
        return symbol, pl.DataFrame()  # Return an empty DataFrame on failure

    async def _fetch_all_data(self):
        async with aiohttp.ClientSession() as session:
            tasks = [self._fetch_stock_data(session, symbol) for symbol in self.symbols]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Save each DataFrame to local storage
            for result in results:
                if isinstance(result, Exception):
                    logging.error(f'Error occurred: {result}')
                    continue

                symbol, df = result

                if df.height == 0:
                    logging.error(f'No data for symbol: {symbol}. Skipping saving.')
                    continue

                if 'label' in df.columns:
                    df = df.drop(['label'])
                df = df.sort(by='date')

                self.data_handler.write_parquet(df, f'{symbol}_data.parquet')
                logging.info(f'Saved data for symbol: {symbol}')

    def update_data(self):
        if asyncio.get_event_loop().is_running():
            return asyncio.ensure_future(self._fetch_all_data())
        else:
            asyncio.run(self._fetch_all_data())
