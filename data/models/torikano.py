import numpy as np
import polars as pl
from data.models.general import DataGatherer, DataStore
from _secrets import FMP_API_KEY
from data.models.prices import PricesDataHandler
from data.models.profile import ProfileDataHandler
from collections import defaultdict



class TorikanoDataProcessor:
    def __init__(self, data_store):
        self.data_store = data_store
        self.sub_directory = "financial_statements"
        self.data_cache = defaultdict(pl.DataFrame)
        self.sectors = pl.DataFrame

    def build_sector_binary_frame(self):
        # Load processed profiles
        all_profiles = self.data_store.read_parquet("processed", "all_profiles.parquet")

        # # Get unique sectors
        # unique_sectors = all_profiles.select(pl.col("sector").unique()).to_series().to_list()
        # unique_symbols  = all_profiles.select(pl.col("symbol").unique()).to_series().to_list()

        all_profiles_no_null = all_profiles.drop_nulls(subset=['sector', 'symbol'])
        all_profiles_no_null = all_profiles_no_null.with_columns(
            pl.lit(1).alias('indicator')
        )


        binary_df = all_profiles_no_null.pivot(
            index='symbol',  # Rows are indexed by 'stock'
            columns='sector',  # Columns are created based on unique values in 'sector'
            values ='indicator'
        )

        binary_df =  binary_df.fill_null(0)
        self.sectors = binary_df
        return binary_df


    def build_returns_df(self):
        returns = self.data_store.read_parquet("core_data", "total_return.parquet")
        returns_melted = self.melt_data_and_rename(returns, "asset_returns")

        self.asset_returns = returns_melted
        return returns_melted

    def melt_data_and_rename(self, dataframe, value_column_name):
        metled_frame = dataframe.melt(id_vars="date")
        metled_frame = metled_frame.rename({"variable":"symbol", "value":value_column_name})
        return metled_frame


    def build_ratio_dfs(self):
        ptb = self.data_store.read_parquet("core_data", "ptb.parquet")
        ptb_melt = self.melt_data_and_rename(ptb, "book_price")
        stp = self.data_store.read_parquet("core_data", "stp.parquet")
        stp_melt = self.melt_data_and_rename(stp, "sales_price")
        cftp = self.data_store.read_parquet("core_data", "cftp.parquet")
        cftp_melt = self.melt_data_and_rename(cftp, "cf_price")
        mkt_cap = self.data_store.read_parquet("core_data", "marketcap.parquet")
        mkt_cap_melt = self.melt_data_and_rename(mkt_cap, "market_cap")

        return {
            "ptb": ptb_melt,
            "stp": stp_melt,
            "cftp": cftp_melt,
            "market_cap": mkt_cap_melt
        }

    def combine_all_data(self, ptb, stp, cfp, mkt_cap, asset_returns):
        # Join all DataFrames on 'date' and 'symbol'
        combined_df = ptb.join(stp, on=['date', 'symbol'], how='left')
        combined_df = combined_df.join(cfp, on=['date', 'symbol'], how='left')
        combined_df = combined_df.join(mkt_cap, on=['date', 'symbol'], how='left')
        combined_df = combined_df.join(asset_returns, on=['date', 'symbol'], how='left')

        return combined_df

    def fill_nan(self,df: pl.DataFrame | pl.LazyFrame, columns: tuple[str, ...], sort_col: str):
        return (
            df.lazy()
            .with_columns([pl.col(f).cast(float).alias(f) for f in columns])
            .with_columns(
                [
                    pl.when(
                        (pl.col(f).abs() == np.inf)
                        | (pl.col(f) == np.nan)
                        | (pl.col(f).is_null())
                        | (pl.col(f).cast(str) == "NaN")
                    )
                    .then(None)
                    .otherwise(pl.col(f))
                    .alias(f)
                    for f in columns
                ]
            )
            .sort(by=sort_col)).collect()

    def sanitise_data_types(self,
            df: pl.DataFrame | pl.LazyFrame, features: tuple[str, ...], sort_col: str, over_col: str, fill_cols: tuple[str, ...],
    ) -> pl.LazyFrame:
        """Cast feature columns to numeric (float), convert NaN and inf values to null, then forward fill nulls
        for each column of `features`, sorted on `sort_col` and partitioned by `over_col`.

        Parameters
        ----------
        df: Polars DataFrame or LazyFrame containing columns `sort_col`, `over_col` and each of `features`
        features: collection of strings indicating which columns of `df` are the feature values
        sort_col: str column of `df` indicating how to sort
        over_col: str column of `df` indicating how to partition

        Returns
        -------
        Polars LazyFrame containing the original columns with cleaned feature data
        """
        try:
            # eagerly check all `features`, `sort_col`, `over_col` present: can't catch ColumNotFoundError in lazy context
            assert all(c in df.columns for c in features + (sort_col, over_col))
            return (
                df.lazy()
                .with_columns([pl.col(f).cast(float).alias(f) for f in features])
                .with_columns(
                    [
                        pl.when(
                            (pl.col(f).abs() == np.inf)
                            | (pl.col(f) == np.nan)
                            | (pl.col(f).is_null())
                            | (pl.col(f).cast(str) == "NaN")
                        )
                        .then(None)
                        .otherwise(pl.col(f))
                        .alias(f)
                        for f in features
                    ]
                )
                .sort(by=sort_col)
            # Rather than ffill for returns, we use min_periods - alternative is to drop over days
            # where there are no returns, given all the stocks are in the same country. Probably
            # Just holidays?
            .with_columns([pl.col(f).forward_fill().over(over_col).alias(f) for f in fill_cols])
            )
        except AttributeError as e:
            raise TypeError("`df` must be a Polars DataFrame | LazyFrame, but it's missing required attributes") from e
        except AssertionError as e:
            raise ValueError(f"`df` must have all of {[over_col, sort_col] + list(features)} as columns") from e


    def build_required_data(self, start_date):
        returns = self.build_returns_df()
        ratios = self.build_ratio_dfs()
        all_data = self.combine_all_data(ratios["ptb"], ratios["stp"],ratios["cftp"],ratios["market_cap"], returns)
        filtered_df = all_data.filter(pl.col('date') >= start_date)
        filtered_df = self.sanitise_data_types(filtered_df,
                                              features=('book_price', 'sales_price', 'cf_price', 'market_cap', 'asset_returns'),
                                              sort_col="date", over_col="symbol", fill_cols=('book_price', 'sales_price', 'cf_price', 'market_cap')).collect()
        return filtered_df


