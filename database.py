import os
import polars as pl
import pandas as pd
class DataHandler:
    def __init__(self, folder_name='local_database'):
        self.folder_name = folder_name
        if not os.path.exists(self.folder_name):
            os.makedirs(self.folder_name)

    def get_filepath(self, filename):
        return os.path.join(self.folder_name, filename)

    def read_parquet(self, filename, engine):
        filepath = self.get_filepath(filename)
        if engine=="polars":
           return pl.read_parquet(filepath)

        elif engine=="pandas":
            return pd.read_parquet(filepath, engine='pyarrow')  # You can also use 'fastparquet'


    def write_parquet(self, df, filename):
        filepath = self.get_filepath(filename)
        df.write_parquet(filepath)

    def read(self, symbol, engine="polars"):
        filename = f'{symbol}_data.parquet'
        return self.read_parquet(filename, engine)
