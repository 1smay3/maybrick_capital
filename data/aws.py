import pandas as pd
import boto3
import os
from _secrets import AWP_ACCESS_KEY, AWP_SECRET_KEY

class AWSHandler:
    def __init__(self, aws_access_key_id, aws_secret_access_key, bucket_name, s3_directory):
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )
        self.bucket_name = bucket_name
        self.s3_directory = s3_directory

    def load_parquet(self, file_path):
        """Load a Parquet file from the local file system into a Pandas DataFrame."""
        try:
            df = pd.read_parquet(file_path)
            print(f"Loaded Parquet file: {file_path}")
            return df
        except Exception as e:
            print(f"Failed to load Parquet file: {file_path}. Error: {e}")
            return None

    def save_parquet(self, file_path, s3_file_name=None):
        """
        Upload a Parquet file to the specified S3 bucket.
        Optionally provide a custom S3 file name, otherwise the local file name is used.
        """
        try:
            # Use the local file name if no S3 file name is provided
            if not s3_file_name:
                s3_file_name = os.path.basename(file_path)

            s3_path = os.path.join(self.s3_directory, s3_file_name)

            # Upload the file to S3
            self.s3_client.upload_file(file_path, self.bucket_name, s3_path)
            print(f"Uploaded {file_path} to s3://{self.bucket_name}/{s3_path}")
        except Exception as e:
            print(f"Failed to upload file: {file_path} to S3. Error: {e}")

    def save_dataframe_to_parquet(self, df, s3_file_name):
        """Save a Pandas DataFrame as a Parquet file and upload it to S3."""
        try:
            # Create a temporary file for the DataFrame
            temp_file_path = f"/tmp/{s3_file_name}"
            df.to_parquet(temp_file_path)

            # Upload the parquet file to S3
            self.save_parquet(temp_file_path, s3_file_name)

            # Optionally, delete the local temp file after upload (if necessary)
            os.remove(temp_file_path)
            print(f"Temporary file {temp_file_path} deleted.")
        except Exception as e:
            print(f"Failed to save DataFrame to Parquet and upload to S3. Error: {e}")

# Initialize the AWSHandler
aws_handler = AWSHandler(
    aws_access_key_id=AWP_ACCESS_KEY,
    aws_secret_access_key=AWP_SECRET_KEY,
bucket_name="maybrick-capital-ldn",
s3_directory="prices")
