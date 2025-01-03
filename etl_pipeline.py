import pandas as pd
import boto3
from sqlalchemy import create_engine
from datetime import datetime
import config  # Import AWS and MySQL credentials

# MySQL connection URL
DATABASE_URI = f"mysql+pymysql://{config.MYSQL_USERNAME}:{config.MYSQL_PASSWORD}@{config.MYSQL_HOST}:{config.MYSQL_PORT}/{config.MYSQL_DB}"
engine = create_engine(DATABASE_URI)

# S3 Client
s3_client = boto3.client(
    's3',
    aws_access_key_id=config.AWS_ACCESS_KEY,
    aws_secret_access_key=config.AWS_SECRET_KEY
)

def fetch_new_data_from_s3():
    """Fetch the latest CSV files from the S3 bucket."""
    response = s3_client.list_objects_v2(Bucket=config.S3_BUCKET_NAME, Prefix=config.S3_FOLDER_PATH)
    files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.csv')]
    return files

def process_and_store_data(files):
    """Read, process, and store the data into MySQL."""
    for file in files:
        obj = s3_client.get_object(Bucket=config.S3_BUCKET_NAME, Key=file)
        df = pd.read_csv(obj['Body'])
        
        # Preprocessing: Handle dates and clean data
        df['admission_date'] = pd.to_datetime(df['admission_date'])
        df['discharge_date'] = pd.to_datetime(df['discharge_date'])
        
        # Store in MySQL
        df.to_sql('inpatient_records', con=engine, if_exists='append', index=False)

def run_etl():
    files = fetch_new_data_from_s3()
    process_and_store_data(files)
    print(f"{len(files)} new files processed at {datetime.now()}")

if __name__ == "__main__":
    run_etl()
