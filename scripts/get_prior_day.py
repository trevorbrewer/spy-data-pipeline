import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from io import StringIO
import boto3
import pandas as pd

# Load environment variables
load_dotenv()

# Setup Alpaca client
ALPACA_API_KEY = os.getenv("ALPACA_KEY_ID")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")
client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)

# Set S3 details
S3_BUCKET_NAME = 'spy-data-min'  # Replace with your S3 bucket name
S3_FILE_NAME = 'spy_min_data.csv'  # Change to your file name if different

# Initialize the S3 client
s3 = boto3.client('s3')

# Define yesterday's date range (Eastern Time assumed)
yesterday = datetime.now() - timedelta(days=1)
start = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
end = yesterday.replace(hour=23, minute=59, second=59, microsecond=0)

# Build request
request = StockBarsRequest(
    symbol_or_symbols="SPY",
    start=start,
    end=end,
    timeframe=TimeFrame.Minute,
    feed="sip"
)

# Fetch data
bars = client.get_stock_bars(request)
df_new_data = bars.df.reset_index()

# Function to load the current CSV file from S3
def load_existing_csv_from_s3(bucket_name, file_name):
    try:
        # Download file from S3
        csv_file_obj = s3.get_object(Bucket=bucket_name, Key=file_name)
        csv_file_body = csv_file_obj['Body'].read().decode('utf-8')
        df_existing = pd.read_csv(StringIO(csv_file_body), index_col='timestamp', parse_dates=True)
        return df_existing
    except Exception as e:
        print(f"Error loading file from S3: {e}")
        return pd.DataFrame()  # Return an empty dataframe if not found

# Function to upload the updated CSV to S3
def upload_to_s3(df, bucket_name, file_name):
    try:
        # Convert DataFrame to CSV in memory
        csv_buffer = StringIO()
        df.to_csv(csv_buffer)
        csv_buffer.seek(0)

        # Upload to S3
        s3.put_object(Bucket=bucket_name, Key=file_name, Body=csv_buffer.getvalue())
        print(f"Successfully uploaded {file_name} to S3.")
    except Exception as e:
        print(f"Error uploading file to S3: {e}")


# Load the existing CSV file from S3
df_existing = load_existing_csv_from_s3(S3_BUCKET_NAME, S3_FILE_NAME)

# Append the new data to the existing data
df_combined = pd.concat([df_existing, df_new_data])

# Upload the updated file to S3
upload_to_s3(df_combined, S3_BUCKET_NAME, S3_FILE_NAME)