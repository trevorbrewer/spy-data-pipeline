import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame 
from io import StringIO
import boto3
import pandas as pd
import pytz

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

# Set timezone to Eastern
eastern = pytz.timezone('US/Eastern')

# Get yesterday's date in Eastern Time
now_eastern = datetime.now(tz=eastern)
yesterday = now_eastern - timedelta(days=2)

# Set start to 4:00 AM and end to 8:00 PM (cover full extended hours)
start = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
end = yesterday.replace(hour=23, minute=59, second=59, microsecond=0)

# Create request
request = StockBarsRequest(
    symbol_or_symbols=["SPY"],
    start=start,
    end=end,
    timeframe=TimeFrame.Minute,
    feed="sip",
    include_extended_hours=True
)


# Fetch data
bars = client.get_stock_bars(request)
df_new_data = bars.df.reset_index()

# reorder to match format

# Function to upload the updated CSV to S3
def upload_to_s3(df, bucket_name, file_name):
    try:
        # Convert DataFrame to CSV in memory
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        # csv_buffer.seek(0)

        # Upload to S3
        s3.put_object(Bucket=bucket_name, Key=file_name, Body=csv_buffer.getvalue())
        print(f"Successfully uploaded {file_name} to S3.")
    except Exception as e:
        print(f"Error uploading file to S3: {e}")


def download_s3_file(local_path='data/spy_minute_data.csv'):
    bucket = os.getenv("S3_BUCKET_NAME")
    s3_key = os.getenv("S3_DATA_PATH")

    s3 = boto3.client("s3")
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    s3.download_file(bucket, s3_key, local_path)
    return local_path


# Load the existing CSV file from S3
data_path = download_s3_file()
df_existing = pd.read_csv(data_path,dtype={'symbol':'str','timestamp':'str','open':'float64','high':'float64','low':'float64',
                                      'close':'float64','volume':'int64'},low_memory=False)

# Append the new data to the existing data
df_combined = pd.concat([df_existing, df_new_data])

# Upload the updated file to S3
upload_to_s3(df_combined, S3_BUCKET_NAME, S3_FILE_NAME)