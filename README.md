# Airflow with Google Cloud Integration

This project sets up an Apache Airflow environment with Google Cloud integration using Docker containers. It includes a complete setup with Airflow webserver, scheduler, workers, and all necessary dependencies.

## Prerequisites

- Docker Engine (20.10.0 or later)
- Docker Compose (v2.0.0 or later)
- Google Cloud SDK
- At least 4GB of RAM
- At least 2 CPU cores
- At least 10GB of free disk space

## Project Structure

```
.
├── dags/                   # Your Airflow DAG files
├── logs/                   # Airflow logs
├── plugins/                # Custom Airflow plugins
├── config/                 # Configuration files
├── Dockerfile             # Custom Airflow image definition
├── docker-compose.yml     # Docker services configuration
├── requirements.txt       # Python package dependencies
└── service_account.json   # Google Cloud service account credentials
```

## Setup Instructions

1. **Clone the Repository**
   ```bash
   git clone git@github.com:ryosagisu/vigilant-octo-spork.git
   cd vigilant-octo-spork
   ```

2. **Set Up Google Cloud Credentials**
   - Place your Google Cloud service account JSON key file in the root directory as `service_account.json`
   - Ensure the service account has the necessary permissions for your GCP services

3. **Create Required Directories**
   ```bash
   mkdir -p ./dags ./logs ./plugins ./config
   ```

4. **Set Environment Variables**
   Create a `.env` file in the project root:
   ```
   AIRFLOW_UID=50000
   AIRFLOW_GID=0
   AIRFLOW_IMAGE_NAME=apache/airflow:2.10.5
   AIRFLOW_PROJ_DIR=.
   _AIRFLOW_WWW_USER_USERNAME=airflow
   _AIRFLOW_WWW_USER_PASSWORD=airflow
   PROJECT_ID=your-project-id
   BUCKET_NAME=your-bucket-name
   ```

5. **Build and Start Services**
   ```bash
   # Build Docker image
   docker-compose build

   # Start all services
   docker-compose up -d
   ```

6. **Access Airflow Web UI**
   - Open your browser and navigate to `http://localhost:8080`
   - Login with:
     - Username: airflow
     - Password: airflow

## Available Services

- **Airflow Webserver**: UI interface (Port 8080)
- **Airflow Scheduler**: DAG scheduling service
- **Airflow Worker**: Task execution service
- **PostgreSQL**: Metadata database
- **Redis**: Message broker
- **Flower**: Celery monitoring tool (Optional, Port 5555)

## Adding DAGs

1. Place your DAG files in the `dags/` directory
2. The scheduler will automatically pick up new DAGs
3. DAGs will appear in the web interface after the next scheduler scan

## Development Guidelines

1. **Custom Dependencies**
   - Add new Python packages to `requirements.txt`
   - Rebuild the Docker image:
     ```bash
     docker-compose build
     docker-compose up -d
     ```

2. **Google Cloud Integration**
   - Use the `google-cloud-*` providers in your DAGs
   - Credentials are automatically loaded from the mounted service account file
   - Example DAG structure:
     ```python
     from airflow import DAG
     from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
     ```

## Monitoring and Maintenance

1. **View Logs**
   ```bash
   docker-compose logs -f airflow-webserver
   docker-compose logs -f airflow-scheduler
   ```

2. **Restart Services**
   ```bash
   docker-compose restart airflow-webserver
   docker-compose restart airflow-scheduler
   ```

3. **Scale Workers**
   ```bash
   docker-compose up -d --scale airflow-worker=3
   ```

## Troubleshooting

1. **Permission Issues**
   - Ensure AIRFLOW_UID is set in your .env file
   - Check directory permissions: `ls -la`
   - Run: `sudo chown -R ${AIRFLOW_UID}:0 logs plugins dags config`

2. **Connection Issues**
   - Verify all services are running: `docker-compose ps`
   - Check service logs: `docker-compose logs <service-name>`
   - Ensure PostgreSQL and Redis are healthy

3. **Google Cloud Authentication**
   - Verify service_account.json is mounted correctly
   - Check permissions on the service account
   - Validate GOOGLE_APPLICATION_CREDENTIALS environment variable

## Security Notes

- Change default airflow/airflow credentials in production
- Secure your service_account.json file
- Never commit credentials to version control
- Use appropriate network security in production
