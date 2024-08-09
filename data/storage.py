import logging
import os
import pandas as pd
import polars as pl
from data.models.symbols import get_sp500_symbols
from constants import ROOT_DIR, FLOAT_FIELDS_PRICES

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class DataHandler:
    def __init__(self, folder_name='local_store', engine="polars"):
        self.folder_name = folder_name
        self.engine = engine
        self.symbols = get_sp500_symbols()  # Fetch symbols during initialization
        self.all_symbols = {}

        # Set the full path using ROOT_DIR
        self.folder_path = os.path.join(ROOT_DIR, "data", self.folder_name)


        # Log the symbols
        logging.info(f"Fetched S&P 500 symbols: {self.symbols}")


    def get_filepath(self, filename):
        return os.path.join(self.folder_path, filename)

    def read_parquet(self, filename, engine=None):
        engine = engine or self.engine
        filepath = self.get_filepath(filename)

        if not os.path.exists(filepath):
            logging.error(f"Failed to read {filename}: File does not exist in {self.folder_path}")
            # Log available files in the directory
            available_files = os.listdir(self.folder_path)
            logging.info(f"Available files in {self.folder_path}: {available_files}")
            return None

        try:
            if engine == "polars":
                return pl.read_parquet(filepath)
            elif engine == "pandas":
                return pd.read_parquet(filepath, engine='pyarrow')  # You can also use 'fastparquet'
            else:
                raise ValueError("Unsupported engine. Use 'polars' or 'pandas'.")
        except Exception as e:
            logging.error(f"Failed to read {filename}: {e}")
            return None

    def write_parquet(self, df, filename):
        filepath = self.get_filepath(filename)
        df.write_parquet(filepath)

    def read(self, symbol, engine=None):
        engine = engine or self.engine
        if symbol in self.all_symbols:
            return self.all_symbols[symbol]
        else:
            filename = f'{symbol}_data.parquet'
            data = self.read_parquet(filename, engine=engine)
            if data is not None:
                self.all_symbols[symbol] = data
            return data

    def read_all(self):
        """Load and cache data for all symbols."""
        for symbol in self.symbols:
            self.all_symbols[symbol] = self.read(symbol)

    def _get_list_of_field_frames(self, field):
        """Fetch and process data for a specific field using cached data."""
        dfs = []
        for symbol, df in self.all_symbols.items():
            if field in df.columns:
                data = df[["date", field]].rename({field: symbol})
                dfs.append(data)

        return dfs

    def get_field(self, field):
        list_of_frames = self._get_list_of_field_frames(field)
        merged_df = list_of_frames[0]

        # Join remaining DataFrames on 'date'
        for df in list_of_frames[1:]:
            merged_df = merged_df.join(df, on="date", how="outer", coalesce=True)

        return merged_df
