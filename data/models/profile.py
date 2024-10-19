import polars as pl
from data.models.general import GenericDataHandler


class ProfileDataHandler(GenericDataHandler):
    def __init__(self, data_gatherer, data_store):
        sub_directory = "profiles"
        super().__init__(data_gatherer, data_store, sub_directory)
        self.api_key = data_gatherer.api_key
        self.endpoint_url = "https://financialmodelingprep.com/api/v3/{interval}/{symbol}?from=1900-01-01&apikey={api_key}"

    def build_url(self, symbol):
        """Build the URL for fetching data."""
        return self.endpoint_url.format(
            interval="profile", symbol=symbol, api_key=self.api_key
        )

    def process_response(self, data):
        """Process the raw API response and convert it to a Polars DataFrame."""
        return pl.DataFrame(data)

    def update_profile_data(self):
        """Run the async gathering and storing process for profile data."""
        self.update_data(self.build_url, self.process_response)

    def combine_and_save_all_profiles(self):
        """Combine all profile data and save it."""
        if "profiles" not in self.data_cache:
            self.read_raw_data()

        # Apply schema to all profile data
        schema = self.data_cache["profiles"]["profiles_MCD"].schema
        self.data_cache["profiles"] = self.apply_schema_to_frames(
            schema, self.data_cache["profiles"]
        )

        # Concatenate all profiles
        combined_frame = self.concat_frames(self.data_cache["profiles"])

        # Save combined profiles
        return self.save_processed_data(
            combined_frame, "processed/market_data", "all_profiles.parquet"
        )
