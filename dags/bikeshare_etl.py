from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Configuration
BUCKET_NAME = os.getenv('BUCKET_NAME')  # Get from environment
PROJECT_ID = os.getenv('PROJECT_ID')    # Get from environment
DATASET = 'bigquery-public-data.austin_bikeshare'
TABLE = 'bikeshare_trips'

def extract_and_save_data(ds, **kwargs):
    """
    Extract bike share data from BigQuery and save to GCS in partitioned Parquet format.
    
    Args:
        ds (str): The execution date as YYYY-MM-DD
        **kwargs: Additional keyword arguments passed by Airflow
    """
    # Calculate yesterday's date
    execution_date = datetime.strptime(ds, '%Y-%m-%d')
    target_date = execution_date - timedelta(days=1)
    
    # Initialize hooks
    bq_hook = BigQueryHook(project_id=PROJECT_ID)
    gcs_hook = GCSHook()
    
    # Query to extract data
    query = f"""
    SELECT 
        trip_id,
        subscriber_type,
        bikeid,
        start_time,
        end_time,
        start_station_id,
        start_station_name,
        end_station_id,
        end_station_name,
        duration_minutes
    FROM `{DATASET}.{TABLE}`
    WHERE DATE(start_time) = DATE('{target_date.strftime('%Y-%m-%d')}')
    """
    
    print(f"Executing query for date: {target_date.strftime('%Y-%m-%d')}")
    
    # Execute query and get results as DataFrame
    df = bq_hook.get_pandas_df(query)
    
    if df.empty:
        print("No data found for the specified date")
        return
    
    # Convert start_time to datetime if it isn't already
    df['start_time'] = pd.to_datetime(df['start_time'])
    
    # Group data by hour and save to separate Parquet files
    for hour, hour_df in df.groupby(df['start_time'].dt.hour):
        # Create partition path
        partition_path = f"bikeshare/{target_date.strftime('%Y-%m-%d')}/{hour:02d}"
        
        # Convert DataFrame to PyArrow Table
        table = pa.Table.from_pandas(hour_df)
        
        # Create Parquet file in memory
        parquet_buffer = pa.BufferOutputStream()
        pq.write_table(table, parquet_buffer)
        
        # Upload to GCS
        gcs_path = f"{partition_path}/data.parquet"
        gcs_hook.upload(
            bucket_name=BUCKET_NAME,
            object_name=gcs_path,
            data=parquet_buffer.getvalue().to_pybytes(),
            mime_type='application/octet-stream'
        )
        
        print(f"Saved partition: gs://{BUCKET_NAME}/{gcs_path}")

# Create the DAG
dag = DAG(
    'bikeshare_etl',
    default_args=default_args,
    description='Extract Austin bikeshare data from BigQuery and save to GCS',
    schedule_interval='0 1 * * *',  # Run at 1 AM daily
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['bikeshare', 'etl'],
)

# Define the task
extract_task = PythonOperator(
    task_id='extract_and_save_data',
    python_callable=extract_and_save_data,
    dag=dag,
)

# Set task dependencies (if we add more tasks later)
extract_task
