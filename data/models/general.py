import aiohttp
import asyncio
from pathlib import Path

import pandas as pd
import polars as pl
import logging
import os
from constants import ROOT_DIR  # Assuming ROOT_DIR is defined in your constants
from data.models.symbols import get_sp500_symbols

# Set up basic configuration for logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DataGatherer:
    def __init__(self, api_key, symbols, rate_limit, data_handler, max_retries=3):
        self.api_key = api_key
        self.symbols = symbols
        self.rate_limit = rate_limit
        self.semaphore = asyncio.Semaphore(rate_limit)
        self.data_handler = data_handler
        self.max_retries = max_retries

    async def _fetch_data(self, session, symbol, url, process_response):
        attempt = 0
        while attempt < self.max_retries:
            async with self.semaphore:
                logging.info(f'Starting to fetch data for symbol: {symbol} (Attempt {attempt + 1})')
                try:
                    async with session.get(url, ssl=False) as response:
                        if response.status == 200:
                            data = await response.json()
                            df = process_response(data)
                            logging.info(f'Fetched data for symbol: {symbol}')
                            return symbol, df
                        elif response.status == 429:  # Rate limit exceeded
                            wait_time = int(response.headers.get('Retry-After', 60))  # Get Retry-After header or default to 60 seconds
                            logging.warning(f'Rate limit exceeded for symbol: {symbol}. Waiting for {wait_time} seconds.')
                            await asyncio.sleep(wait_time)
                        else:
                            response.raise_for_status()
                except aiohttp.ClientResponseError as e:
                    logging.error(f'Client response error for symbol: {symbol}. Error: {e}')
                except aiohttp.ClientError as e:
                    logging.error(f'Client error for symbol: {symbol}. Error: {e}')
                attempt += 1

        logging.error(f'Failed to fetch data for symbol: {symbol} after {self.max_retries} attempts.')
        return symbol, pl.DataFrame()  # Return an empty DataFrame on failure

    async def _fetch_all_data(self, build_url, process_response, file_suffix):
        async with aiohttp.ClientSession() as session:
            tasks = [self._fetch_data(session, symbol, build_url(symbol), process_response) for symbol in self.symbols]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in results:
                if isinstance(result, Exception):
                    logging.error(f'Error occurred: {result}')
                    continue

                symbol, df = result

                if df.height == 0:
                    logging.error(f'No data for symbol: {symbol}. Skipping saving.')
                    continue

                self.data_handler.write_parquet(df, file_suffix, f"{symbol}.parquet")
                logging.info(f'Saved data for symbol: {symbol}')

    def update_data(self, build_url, process_response, file_suffix):
        if asyncio.get_event_loop().is_running():
            return asyncio.ensure_future(self._fetch_all_data(build_url, process_response, file_suffix))
        else:
            asyncio.run(self._fetch_all_data(build_url, process_response, file_suffix))


# Set up basic configuration for logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class DataStore:
    def __init__(self, base_location='data/local_store', engine="polars"):
        self.base_location = Path(base_location)
        self.engine = engine
        self.folder_path = os.path.join(ROOT_DIR, self.base_location)
        self.all_data = {}
        self.symbols = get_sp500_symbols()  # Fetch symbols during initialization


        # Log the initialization
        logging.info(f"Initialized DataStore with base folder: {self.folder_path}")

    def _get_full_path(self, sub_directory, filename):
        """Construct the full file path including subdirectory."""
        subdir_path = os.path.join(self.folder_path, sub_directory)
        if not os.path.exists(subdir_path):
            os.makedirs(subdir_path)
        return os.path.join(subdir_path, filename)

    def read_parquet(self, sub_directory, filename, engine=None):
        engine = engine or self.engine
        filepath = self._get_full_path(sub_directory, filename)

        if not os.path.exists(filepath):
            logging.error(f"Failed to read {filename}: File does not exist in {sub_directory}")
            available_files = os.listdir(os.path.dirname(filepath))
            logging.info(f"Available files in {sub_directory}: {available_files}")
            return None

        try:
            if engine == "polars":
                return pl.read_parquet(filepath)
            elif engine == "pandas":
                return pd.read_parquet(filepath, engine='pyarrow')
            else:
                raise ValueError("Unsupported engine. Use 'polars' or 'pandas'.")
        except Exception as e:
            logging.error(f"Failed to read {filename}: {e}")
            return None

    def write_parquet(self, df, sub_directory, filename):
        filepath = self._get_full_path(sub_directory, filename)
        try:
            df.write_parquet(filepath)
            logging.info(f"Successfully wrote data to {filepath}")
        except Exception as e:
            logging.error(f"Failed to write {filename}: {e}")

    def read(self, sub_directory, filename, engine=None):
        # Use the provided engine or fallback to the default one
        engine = engine or self.engine

        # Construct the file path
        filepath = self._get_full_path(sub_directory, f'{filename}.parquet')

        if engine == "polars":
            return pl.read_parquet(filepath)
        elif engine == "pandas":
            return pd.read_parquet(filepath, engine='pyarrow')
        else:
            raise ValueError("Unsupported engine. Use 'polars' or 'pandas'.")

    def read_all(self, sub_directory):
        self.all_data = {}
        """Load and cache all data files from a specific subdirectory."""
        # Create a Path object for the subdirectory
        subdir_path = Path(self.folder_path) / sub_directory

        # Get a set of existing file stems in the subdirectory
        existing_files = set(filepath.stem for filepath in subdir_path.glob('*.parquet'))

        # Check for missing symbols
        missing_symbols = set(self.symbols).difference(existing_files)
        extra_files = existing_files.difference(set(self.symbols))

        if missing_symbols:
            print(f"Missing files for symbols: {missing_symbols}")
        if extra_files:
            print(f"Extra files found: {extra_files}")

        # Iterate through each parquet file in the subdirectory
        for filepath in subdir_path.glob('*.parquet'):
            # Read parquet file and cache it
            data = self.read(sub_directory, filepath.stem)
            if data is not None:
                self.all_data[f'{sub_directory}_{filepath.stem}'] = data

        return self.all_data
