def pct_change(dataframe, lookback):
    def percentage_change(data):
        return data.diff(lookback) / data.shift(lookback)

    pct_change = dataframe.with_columns([
        percentage_change(dataframe[col]).alias(f"{col}") for col in dataframe.columns if col != 'date'
    ])

    return pct_change
