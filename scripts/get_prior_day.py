import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from io import StringIO
import boto3
import pandas as pd
from s3_utils.s3_loader import download_s3_file

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


# Build request
request = StockBarsRequest(
    symbol_or_symbols="SPY",
    start=datetime.now() - timedelta(days=2),
    end=datetime.now() - timedelta(days=1),
    timeframe=TimeFrame.Minute,
    feed="sip",  # <- goes here
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


# Load the existing CSV file from S3
data_path = download_s3_file()
df_existing = pd.read_csv(data_path,dtype={'symbol':'str','timestamp':'str','open':'float64','high':'float64','low':'float64',
                                      'close':'float64','volume':'int64'},low_memory=False)

print(df_new_data.head())
print(df_existing.head())

# # Append the new data to the existing data
# df_combined = pd.concat([df_existing, df_new_data])

# # Upload the updated file to S3
# upload_to_s3(df_combined, S3_BUCKET_NAME, S3_FILE_NAME)