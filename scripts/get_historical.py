import os
from dotenv import load_dotenv
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
import boto3
from io import StringIO

# Load environment variables
load_dotenv()

# Set up Alpaca client
ALPACA_API_KEY = os.getenv("ALPACA_KEY_ID")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")
client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)

# Set S3 details
S3_BUCKET_NAME = 'spy-data-min'  # Replace with your S3 bucket name
S3_FILE_NAME = 'spy_min_data.csv'  # You can change the file name

# Function to upload DataFrame to S3 as a CSV file
def upload_to_s3(df):
    # Convert DataFrame to CSV in memory
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    # Create boto3 client for S3
    s3_client = boto3.client('s3')

    # Upload to S3
    s3_client.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=S3_FILE_NAME,
        Body=csv_buffer.getvalue()
    )
    print(f"File uploaded to S3: {S3_BUCKET_NAME}/{S3_FILE_NAME}")

# Define request with correct 'feed' placement
request_params = StockBarsRequest(
    symbol_or_symbols="SPY",
    start="2016-01-01",
    end="2025-04-12",
    timeframe=TimeFrame.Minute,
    feed="sip"  # <- goes here
)

# Fetch bars
bars = client.get_stock_bars(request_params)

# Convert to DataFrame
df = bars.df.reset_index()

# Optional: filter extended hours
# Alpaca's SIP feed includes extended hours by default
# You can keep all or filter with your own logic

# Save to CSV
upload_to_s3(df)

# print(f"Saved {len(df)} rows to S3")
