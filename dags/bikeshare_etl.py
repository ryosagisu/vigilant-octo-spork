from datetime import datetime, timedelta
import os
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from airflow import DAG
from airflow.models.param import Param
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
DEFAULT_TARGET_DATE = '-1'
BUCKET_NAME = os.getenv('BUCKET_NAME')  # Get from environment
PROJECT_ID = os.getenv('PROJECT_ID')    # Get from environment
MAX_WORKERS = 50  # Maximum number of concurrent uploads

def get_target_date(ds, target_date_str) -> datetime:
    if target_date_str and target_date_str != DEFAULT_TARGET_DATE:
        try:
            # Try parsing the user-supplied target_date param
            return datetime.strptime(target_date_str, "%Y-%m-%d")
        except ValueError:
            # Invalid date format — log and fall back
            logging.warning(f"Invalid 'target_date' param: {target_date_str}. Falling back to ds - 1 day.")
    
    # Fallback logic if param is not provided or invalid
    execution_date = datetime.strptime(ds, "%Y-%m-%d")
    return execution_date - timedelta(days=1)

def upload_partition(bucket_name: str, date: str, hour: int, group_df: pd.DataFrame, gcs_hook: GCSHook) -> str:
    """
    Upload a single partition to GCS.
    
    Args:
        bucket_name: GCS bucket name
        date: Partition date
        hour: Hour of the day
        group_df: DataFrame for this partition
        gcs_hook: GCS Hook instance
    
    Returns:
        str: GCS path where the file was uploaded
    """
    # Drop partition columns before saving
    df_to_save = group_df.drop(columns=['datadate', 'datahour'])
    
    # Convert DataFrame to PyArrow Table
    table = pa.Table.from_pandas(df_to_save, preserve_index=False)
    
    # Create Parquet file in memory
    parquet_buffer = pa.BufferOutputStream()
    pq.write_table(table, parquet_buffer)
    
    # Upload to GCS
    gcs_path = f"bikeshare_trips/datadate={date}/datahour={hour:02d}/data.parquet"
    gcs_hook.upload(
        bucket_name=bucket_name,
        object_name=gcs_path,
        data=parquet_buffer.getvalue().to_pybytes(),
        mime_type='application/octet-stream'
    )
    
    return gcs_path

def extract_and_save_data(ds, **kwargs):
    """
    Extract bike share data from BigQuery and save to GCS in partitioned Parquet format.
    
    Args:
        ds (str): The execution date as YYYY-MM-DD
        **kwargs: Additional keyword arguments passed by Airflow
    """

    logging.info(f"PROJECT_ID = {PROJECT_ID}, BUCKET_NAME = {BUCKET_NAME}")

    params: ParamsDict = kwargs.get("params", {})
    target_date_str = params.get("target_date")
    target_date = get_target_date(ds, target_date_str)
    formatted_date = target_date.strftime('%Y-%m-%d')
    logging.info(f"Executing query for date: {formatted_date}")

    # Initialize hooks
    bq_hook = BigQueryHook(project_id=PROJECT_ID, use_legacy_sql=False)
    gcs_hook = GCSHook()

    # Query to extract data
    query = f"""
    SELECT 
        trip_id,
        subscriber_type,
        bike_id,
        bike_type,
        start_time,
        start_station_id,
        start_station_name,
        end_station_id,
        end_station_name,
        duration_minutes
    FROM bigquery-public-data.austin_bikeshare.bikeshare_trips"""

    if not params.get('is_full_scan'):
        query = query + f"""
        WHERE DATE(start_time) = DATE('{formatted_date}')
        """

    logging.info(f"Query: {query}")

    # Execute query and get results as DataFrame
    df = bq_hook.get_pandas_df(query)

    if df.empty:
        logging.warning("No data found for the specified date")
        return

    # Convert start_time to datetime if it isn't already
    df['start_time'] = pd.to_datetime(df['start_time'])
    
    # Add date and hour columns for grouping
    df['datadate'] = df['start_time'].dt.date
    df['datahour'] = df['start_time'].dt.hour
    
    # Prepare upload tasks
    upload_tasks = []
    for (date, hour), group_df in df.groupby(['datadate', 'datahour']):
        upload_tasks.append((date, hour, group_df))
    
    logging.info(f"Preparing to upload {len(upload_tasks)} partitions")
    
    # Upload files in parallel
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_path = {
            executor.submit(
                upload_partition, 
                BUCKET_NAME, 
                date, 
                hour, 
                group_df,
                gcs_hook
            ): (date, hour)
            for date, hour, group_df in upload_tasks
        }
        
        # Process completed uploads
        for future in as_completed(future_to_path):
            date, hour = future_to_path[future]
            try:
                gcs_path = future.result()
                logging.info(f"Successfully uploaded: gs://{BUCKET_NAME}/{gcs_path}")
            except Exception as e:
                logging.error(f"Error uploading partition for date={date}, hour={hour}: {str(e)}")
                raise

# Create the DAG
dag = DAG(
    'bikeshare_etl',
    params={
        'target_date': Param(type='string', default=DEFAULT_TARGET_DATE, description='The execution date as YYYY-MM-DD'),
        'is_full_scan': Param(type='boolean', default=False, description='Whether to perform a full scan of the data')
    },
    default_args=default_args,
    description='Extract Austin bikeshare data from BigQuery and save to GCS',
    schedule_interval='0 1 * * *',  # Run at 1 AM daily
    start_date=datetime(2025, 1, 1),
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
