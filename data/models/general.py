import aiohttp
import asyncio
from pathlib import Path
from typing import List, Callable, Union, Dict, Optional
import pandas as pd
import polars as pl
import logging
import os
from constants import ROOT_DIR
from data.models.symbols import get_sp500_symbols
from datetime import datetime as dt
from pathlib import WindowsPath

# Set up basic configuration for logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# TODO: This has become really messy from rushed incremental functionality, and needs refactoring
class DataStore:
    def __init__(self, base_location="data/local_store", engine="polars"):
        self.base_location: Path = Path(base_location)
        self.engine: str = engine
        self.folder_path: str = os.path.join(ROOT_DIR, self.base_location)
        self.all_data: dict = {}
        self.symbols: List[str] = get_sp500_symbols() # Fetch symbols during initialization

        # Log the initiaization
        logging.info(f"Initialized DataStore with base folder: {self.folder_path}")

    def _get_full_path(self, sub_directory: str, filename: str) -> str:
        """Construct the full file path including subdirectory."""
        subdir_path = os.path.join(self.folder_path, sub_directory)
        return os.path.join(subdir_path, filename)

    def read_parquet(
        self, sub_directory: str, filename: str, engine="polars"
    ) -> Optional[Union[pl.DataFrame, pd.DataFrame]]:
        engine = engine or self.engine
        filepath = self._get_full_path(sub_directory, filename)

        if not os.path.exists(filepath):
            logging.error(
                f"Failed to read {filename}: File does not exist in {sub_directory}"
            )
            available_files = os.listdir(os.path.dirname(filepath))
            logging.info(f"Available files in {sub_directory}: {available_files}")
            return None

        try:
            if engine == "polars":
                return pl.read_parquet(filepath)
            elif engine == "pandas":
                return pd.read_parquet(filepath, engine="pyarrow")
            else:
                raise ValueError("Unsupported engine. Use 'polars' or 'pandas'.")
        except Exception as e:
            logging.error(f"Failed to read {filename}: {e}")
            return None

    def write_parquet(
        self,
        df: Union[pd.DataFrame, pl.DataFrame],
        sub_directory: str,
        filename: str,
        log: bool = True,
    ) -> None:
        filepath = self._get_full_path(sub_directory, filename)
        try:
            df.write_parquet(filepath)
            if log:
                logging.info(f"Successfully wrote data to {filepath}")
        except Exception as e:
            logging.error(f"Failed to write {filename}: {e}")

    def read(
        self,
        sub_directory: str,
        filename: str,
        engine: Optional[str] = None,
        parquet_suffix: Optional[str] = True,
    ) -> Union[pl.DataFrame, pd.DataFrame]:
        # Use the provided engine or fallback to the default one
        engine = engine or self.engine

        clean_filename = f"{filename}.parquet" if parquet_suffix else filename

        # Construct the file path
        filepath = self._get_full_path(sub_directory, f"{clean_filename}")

        if engine == "polars":
            if os.path.isdir(filepath):
                print(filepath, "is a directory!")
            else:
                return pl.read_parquet(filepath)

        elif engine == "pandas":
            return pd.read_parquet(filepath, engine="pyarrow")
        else:
            raise ValueError("Unsupported engine. Use 'polars' or 'pandas'.")

    def real_all_in_directory(
        self, sub_directory: str, parquet_suffix: Union[bool, str] = True
    ) -> Dict[str, Union[pl.DataFrame, pd.DataFrame]]:
        self.all_data = {}
        """Load and cache all data files from a specific subdirectory."""
        # Create a Path object for the subdirectory
        subdir_path = Path(self.folder_path) / sub_directory

        # Get a set of existing file stems in the subdirectory
        if parquet_suffix:
            existing_files = set(
                filepath.stem for filepath in subdir_path.glob("*.parquet")
            )
        else:
            existing_files = set(filepath for filepath in subdir_path.glob("*"))

        # Check for missing symbols - TODO: This doesnt work, as we arent always expecting to load symbols
        missing_symbols = set(self.symbols).difference(existing_files)
        extra_files = existing_files.difference(set(self.symbols))

        if missing_symbols:
            print(f"Missing files for {sub_directory}, symbols: {missing_symbols}")
        if extra_files:
            print(f"Extra files found: {extra_files}")

        # Iterate through each parquet file in the subdirectory
        for filepath in existing_files:
            # Construct the full path
            # Read parquet file and cache it
            data = self.read(sub_directory, filepath, parquet_suffix=parquet_suffix)
            if data is not None:
                self.all_data[f"{sub_directory}_{filepath}"] = data

        return self.all_data

    def read_core_data(self) -> Dict[str, Union[pl.DataFrame, pd.DataFrame]]:
        core_data_dict = self.real_all_in_directory("core_data", parquet_suffix=True)
        clean_core_data_dict = {}
        for k, v in core_data_dict.items():
            clean_core_data_dict[k.replace("core_data_", "")] = v
        return clean_core_data_dict


class DataGatherer:
    def __init__(
        self,
        api_key: str,
        symbols: List[str],
        rate_limit: int,
        data_handler,
        max_retries=3,
    ):
        self.api_key = api_key
        self.symbols = symbols
        self.rate_limit = rate_limit
        self.semaphore = asyncio.Semaphore(rate_limit)
        self.data_handler = data_handler
        self.max_retries = max_retries

    async def _fetch_data(
        self,
        session: aiohttp.ClientSession,
        symbol: str,
        url: str,
        process_response: Callable,
    ) -> tuple[str, pl.DataFrame]:
        attempt = 0
        while attempt < self.max_retries:
            async with self.semaphore:
                logging.info(
                    f"Starting to fetch data for symbol: {symbol} (Attempt {attempt + 1})"
                )
                try:
                    async with session.get(url, ssl=False) as response:
                        if response.status == 200:
                            data = await response.json()
                            df = process_response(data)
                            logging.info(f"Fetched data for symbol: {symbol}")
                            return symbol, df
                        elif response.status == 429:  # Rate limit exceeded
                            wait_time = int(
                                response.headers.get("Retry-After", 60)
                            )  # Get Retry-After header or default to 60 seconds
                            logging.warning(
                                f"Rate limit exceeded for symbol: {symbol}. Waiting for {wait_time} seconds."
                            )
                            await asyncio.sleep(wait_time)
                        else:
                            response.raise_for_status()
                except aiohttp.ClientResponseError as e:
                    logging.error(
                        f"Client response error for symbol: {symbol}. Error: {e}"
                    )
                except aiohttp.ClientError as e:
                    logging.error(f"Client error for symbol: {symbol}. Error: {e}")
                attempt += 1

        logging.error(
            f"Failed to fetch data for symbol: {symbol} after {self.max_retries} attempts."
        )
        return symbol, pl.DataFrame()  # Return an empty DataFrame on failure

    async def _fetch_all_data(
        self,
        build_url: Callable,
        process_response: Callable,
        file_suffix: str,
        date_chunker: Optional[Callable] = False,
    ) -> Dict[str, List[pl.DataFrame]]:
        async with aiohttp.ClientSession() as session:
            if date_chunker:
                for symbol in self.symbols:
                    all_date_chunks = date_chunker(
                        dt(1990, 1, 1)
                    )  # Adjust date as needed
                    tasks = [
                        self._fetch_data(
                            session,
                            symbol,
                            build_url(symbol, date_chunk),
                            process_response,
                        )
                        for date_chunk in all_date_chunks
                    ]

                    chunk_results = await asyncio.gather(*tasks, return_exceptions=True)

                    all_data = []
                    for result in chunk_results:
                        symbol, data = result
                        if data.shape[0]>0:
                            all_data.append(data)
                        else:
                            print("Empty Frame for ", symbol)

                    symbol, df = symbol, pl.concat(all_data)

                    # TODO: Can just have in outer loop
                    if df.height == 0:
                        logging.error(f"No data for symbol: {symbol}. Skipping saving.")
                        continue

                    self.data_handler.write_parquet(
                        df, file_suffix, f"{symbol}.parquet"
                    )
                    logging.info(f"Saved data for symbol: {symbol}")

            else:
                tasks = [
                    self._fetch_data(
                        session, symbol, build_url(symbol), process_response
                    )
                    for symbol in self.symbols
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                for result in results:
                    if isinstance(result, Exception):
                        logging.error(f"Error occurred: {result}")
                        continue

                    symbol, df = result

                    if df.height == 0:
                        logging.error(f"No data for symbol: {symbol}. Skipping saving.")
                        continue

                    self.data_handler.write_parquet(
                        df, file_suffix, f"{symbol}.parquet"
                    )
                    logging.info(f"Saved data for symbol: {symbol}")

    def update_data(
        self,
        build_url: Callable,
        process_response: Callable,
        file_suffix: str,
        date_chunker: Optional[Callable] = None,
    ) -> Optional[asyncio.Future]:
        if asyncio.get_event_loop().is_running():
            return asyncio.ensure_future(
                self._fetch_all_data(
                    build_url, process_response, file_suffix, date_chunker
                )
            )
        else:
            return asyncio.run(
                self._fetch_all_data(
                    build_url, process_response, file_suffix, date_chunker
                )
            )
