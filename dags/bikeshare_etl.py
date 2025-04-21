from datetime import datetime, timedelta
import os
import logging

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

def get_target_date(ds, **kwargs) -> datetime:
    params: ParamsDict = kwargs.get("params", {})
    target_date_str = params.get("target_date")

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

def extract_and_save_data(ds, **kwargs):
    """
    Extract bike share data from BigQuery and save to GCS in partitioned Parquet format.
    
    Args:
        ds (str): The execution date as YYYY-MM-DD
        **kwargs: Additional keyword arguments passed by Airflow
    """

    logging.info(f"PROJECT_ID = {PROJECT_ID}, BUCKET_NAME = {BUCKET_NAME}")

    target_date = get_target_date(ds, **kwargs)
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
    FROM bigquery-public-data.austin_bikeshare.bikeshare_trips
    """

    if not kwargs.get('is_full_scan', True):
        query = query + f"""
        WHERE DATE(start_time) = DATE('{formatted_date}')
        """

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

    # Group data by date and hour and save to separate Parquet files
    for (date, hour), group_df in df.groupby(['datadate', 'datahour']):
        # Convert DataFrame to PyArrow Table
        table = pa.Table.from_pandas(group_df, preserve_index=False)

        # Create Parquet file in memory
        parquet_buffer = pa.BufferOutputStream()
        pq.write_table(table, parquet_buffer)

        # Upload to GCS
        gcs_path = f"bikeshare_trips/datadate={date}/datahour={hour:02d}/data.parquet"
        gcs_hook.upload(
            bucket_name=BUCKET_NAME,
            object_name=gcs_path,
            data=parquet_buffer.getvalue().to_pybytes(),
            mime_type='application/octet-stream'
        )

        logging.info(f"Saved partition: gs://{BUCKET_NAME}/{gcs_path}")

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
