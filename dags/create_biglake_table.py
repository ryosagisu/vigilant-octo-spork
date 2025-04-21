from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
from google.cloud.bigquery_connection_v1 import ConnectionServiceClient
from google.cloud.bigquery_connection_v1.types import Connection, CreateConnectionRequest
from google.cloud.bigquery_connection_v1.types import CloudResourceProperties

# Configuration
PROJECT_ID = os.getenv('PROJECT_ID')
BUCKET_NAME = os.getenv('BUCKET_NAME')

def generate_external_table_query():
    """Generate the SQL query configuration to create or replace the external table."""
    return {
        'query': {
            'query': f"""
            CREATE OR REPLACE EXTERNAL TABLE `{PROJECT_ID}.Spectacle.bikeshare_trips_external`
            WITH PARTITION COLUMNS (
                datadate DATE,
                datahour INT64
            )
            WITH CONNECTION `{PROJECT_ID}.us.biglake-connection`
            OPTIONS (
                uris = ['gs://{BUCKET_NAME}/bikeshare/*'],
                format = 'PARQUET',
                hive_partition_uri_prefix = 'gs://{BUCKET_NAME}/bikeshare/',
                require_hive_partition_filter = false,
                max_staleness = INTERVAL 1 DAY,
                metadata_cache_mode = 'AUTOMATIC'
            )""",
            'useLegacySql': False,
        }
    }

def create_biglake_connection(project_id: str, **kwargs):
    """
    Create a BigLake connection if it doesn't exist.
    """
    client = ConnectionServiceClient()
    parent = f"projects/{project_id}/locations/us"
    connection_id = "biglake-connection"
    
    # Define the connection
    connection = Connection(
        cloud_resource=CloudResourceProperties(),
    )
    
    request = CreateConnectionRequest(
        parent=parent,
        connection_id=connection_id,
        connection=connection
    )
    
    try:
        # Check if connection exists
        existing_conn = client.get_connection(name=f"{parent}/connections/{connection_id}")
        print(f"Connection {existing_conn.name} already exists")
    except Exception:
        try:
            # Create new connection
            result = client.create_connection(request=request)
            print(f"Created connection {result.name}")
        except Exception as e:
            print(f"Error creating connection: {str(e)}")
            raise

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'create_biglake_table',
    default_args=default_args,
    description='Create BigLake external table for bikeshare data',
    schedule_interval='15 1 * * *',  # Run at 1:15 AM daily (after ETL DAG)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['bikeshare', 'biglake'],
)

# Task 1: Create BigLake connection
create_connection = PythonOperator(
    task_id='create_biglake_connection',
    python_callable=create_biglake_connection,
    op_kwargs={'project_id': PROJECT_ID},
    dag=dag,
)

# Task 2: Create or update external table
create_external_table = BigQueryInsertJobOperator(
    task_id='create_external_table',
    configuration=generate_external_table_query(),
    location='US',
    project_id=PROJECT_ID,
    dag=dag,
)

# Set task dependencies
create_connection >> create_external_table
