from data.models.torikano import TorikanoDataProcessor
from data.models.general import DataStore
from datetime import datetime as dt
from toraniko.styles import factor_mom, factor_val, factor_sze
from toraniko.utils import top_n_by_group

from toraniko.model import estimate_factor_returns

data_store = DataStore(base_location='data/local_store', engine="polars")
torikano_data_handler = TorikanoDataProcessor(data_store=data_store)


# TODO: momo broken, insanely shit.
torikano_data = torikano_data_handler.build_required_data(start_date = dt(2000, 1, 4))


mom_df = factor_mom(torikano_data.select("symbol", "date", "asset_returns"), trailing_days=252, winsor_factor=0.01).collect()
mom_df = torikano_data_handler.fill_nan(mom_df, ("mom_score",), "date")
value_df = factor_val(torikano_data.select("date", "symbol", "book_price", "sales_price", "cf_price")).collect()
value_df = torikano_data_handler.fill_nan(value_df, ("val_score", ), "date")
size_df = factor_sze(torikano_data.select("date", "symbol", "market_cap")).collect()
size_df = torikano_data_handler.fill_nan(size_df, ("sze_score",), "date")




style_scores = mom_df.join(value_df, on=["date", "symbol"]).join(size_df, on=["date", "symbol"])
ret_df = torikano_data.select("symbol", "date", "asset_returns")
cap_df = torikano_data.select("date", "symbol", "market_cap")
sector_scores = torikano_data_handler.build_sector_binary_frame()




ddf = (
    ret_df.join(cap_df, on=["date", "symbol"])
    .join(sector_scores, on="symbol")
    .join(style_scores, on=["date", "symbol"])
    .drop_nulls()
)


ddf = (
    top_n_by_group(
        ddf.lazy(),
        400,
        "market_cap",
        ("date",),
        True
    )
    .collect()
    .sort("date", "symbol")
)


returns_df = ddf.select("date", "symbol", "asset_returns")
mkt_cap_df = ddf.select("date", "symbol", "market_cap")
sector_df = ddf.select(["date"] + list(sector_scores.columns))
# You cant have nan in styles:
style_df = ddf.select(style_scores.columns)



fac_df, eps_df = estimate_factor_returns(returns_df, mkt_cap_df, sector_df, style_df, winsor_factor=0.1, residualize_styles=False)

print("-")