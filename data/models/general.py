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
from collections import defaultdict
from typing import Union
import pyarrow as pa
import pyarrow.parquet as pq




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
        self.symbols: List[str] = get_sp500_symbols() # Fetch symbols during initialization # TODO: EXPOSE and CACHE This...

        # Log the initialization
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
            metadata: dict,  # Add metadata as an optional parameter
        log: bool = True,
    ) -> None:
        filepath = self._get_full_path(sub_directory, filename)

        try:
            # Convert DataFrame to PyArrow Table based on the input type
            if isinstance(df, pd.DataFrame):
                arrow_table = pa.Table.from_pandas(df)
            elif isinstance(df, pl.DataFrame):
                arrow_table = df.to_arrow()
            else:
                raise TypeError("Data must be either a pandas or polars DataFrame.")

            # Add metadata to the schema, if provided
            if metadata:
                schema_with_metadata = arrow_table.schema.add_metadata(metadata)
            else:
                schema_with_metadata = arrow_table.schema

            # Write the parquet file with (or without) metadata
            with pq.ParquetWriter(filepath, schema=schema_with_metadata) as writer:
                writer.write_table(arrow_table)

            if log:
                logging.info(f"Successfully wrote data to {filepath}")
        except Exception as e:
            logging.error(f"Failed to write {filename}: {e}")

    def read(
            self,
            sub_directory: str,
            filename: str,
            engine: Optional[str] = None,
            return_metadata: bool = False  # Option to return metadata
    ) -> Union[pl.DataFrame, pd.DataFrame, tuple[pl.DataFrame, dict], tuple[pd.DataFrame, dict]]:
        # Use the provided engine or fallback to the default one
        engine = engine or self.engine

        # Construct the file path
        filepath = self._get_full_path(sub_directory, filename)

        # Handle Polars DataFrame
        if engine == "polars":
            if os.path.isdir(filepath):
                print(filepath, "is a directory!")
            else:
                df = pl.read_parquet(filepath)
                if return_metadata:
                    # Read the metadata using pyarrow
                    parquet_file = pq.ParquetFile(filepath)
                    metadata = parquet_file.metadata.metadata  # Extract metadata
                    metadata_dict = {k.decode(): v.decode() for k, v in metadata.items()}
                    return df, metadata_dict  # Return the DataFrame and metadata
                return df

        # Handle Pandas DataFrame
        elif engine == "pandas":
            df = pd.read_parquet(filepath, engine="pyarrow")
            if return_metadata:
                # Read the metadata using pyarrow
                parquet_file = pq.ParquetFile(filepath)
                metadata = parquet_file.metadata.metadata  # Extract metadata
                metadata_dict = {k.decode(): v.decode() for k, v in metadata.items()}
                return df, metadata_dict  # Return the DataFrame and metadata
            return df

        else:
            raise ValueError("Unsupported engine. Use 'polars' or 'pandas'.")

    def read_all_in_directory(
            self, sub_directory: str, return_metadata: bool
    ) -> Dict[str, Dict[str, Union[pl.DataFrame, pd.DataFrame]]]:
        """Load and cache all data files from a specific subdirectory, returning metadata and data."""
        self.all_data = {}
        # Create a Path object for the subdirectory
        subdir_path = Path(self.folder_path) / sub_directory

        existing_files = set(filepath for filepath in subdir_path.glob("*"))

        # Iterate through each file in the subdirectory
        for filepath in existing_files:
            # Read parquet file and cache it
            data, metadata = self.read(sub_directory, engine=self.engine, filename=filepath, return_metadata=return_metadata)
            if data is not None:
                self.all_data[f"{sub_directory}_{filepath.name}"] = {
                    "metadata": metadata,
                    "data": data
                }

        return self.all_data

    def read_core_data(self) -> Dict[str, Union[pl.DataFrame, pd.DataFrame]]:
        core_data_dict = self.read_all_in_directory("core_data")
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

                    print(symbol)
                    self.data_handler.write_parquet(
                        df, sub_directory=file_suffix, filename=f"{symbol}.parquet", metadata={"symbol":symbol, "recieved_dt":dt.now().strftime("%Y-%m-%d %H:%M:%S")}
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
                        df, sub_directory=file_suffix, filename=f"{symbol}.parquet", metadata={"symbol":symbol, "recieved_dt":dt.now().strftime("%Y-%m-%d %H:%M:%S")}
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

class GenericDataHandler:
    def __init__(self, data_gatherer, data_store, sub_directory):
        self.data_gatherer = data_gatherer
        self.data_store = data_store
        self.sub_directory = sub_directory
        self.data_cache = defaultdict(pl.DataFrame)

    def read_raw_data(self, sub_directory):
        """Load raw data from the data store and cache it."""
        all_data = self.data_store.read_all_in_directory(sub_directory, return_metadata=True)
        # Cache data using sub_directory as key
        self.data_cache[sub_directory] = all_data
        return all_data

    async def gather_and_store_data(self, build_url, process_data):
        """Fetch data for all symbols and store it."""
        await self.data_gatherer._fetch_all_data(
            build_url, process_data, self.sub_directory
        )

    def update_data(self, build_url, process_data):
        """Run the async gathering and storing process."""
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                raise RuntimeError(
                    "Cannot run 'update_data' while another event loop is running"
                )
            else:
                loop.run_until_complete(self.gather_and_store_data(build_url, process_data))
        except RuntimeError as e:
            print(f"RuntimeError: {e}")
            raise e

    def _get_list_of_field_frames(self, key, field):
        """Fetch and process data for a specific field using cached data."""
        if key not in self.data_cache:
            raise ValueError(f"No data available for key: {key}")

        all_frames = self.data_cache[key]
        dfs = []
        for frame_name, frame_data in all_frames.items():
            symbol = frame_data["metadata"]["symbol"]
            underlying_data = frame_data["data"]
            data = underlying_data[["date", field]].rename({field: symbol})
            dfs.append(data)
        return dfs

    def get_field(self, key, field):
        """Get a merged DataFrame of a specific field across all symbols."""
        list_of_frames = self._get_list_of_field_frames(key, field)
        if not list_of_frames:
            return pl.DataFrame()
        merged_df = list_of_frames[0]
        for df in list_of_frames[1:]:
            merged_df = merged_df.join(df, how="full", on="date", coalesce=True)
        no_duplicates_df = merged_df.unique(keep="first", subset="date")
        return no_duplicates_df.sort("date")
