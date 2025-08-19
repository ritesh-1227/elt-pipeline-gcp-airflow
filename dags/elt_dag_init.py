from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.empty import EmptyOperator

# DAG default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define project, dataset, and table details
project_id = 'sage-extension-469317-t5'
dataset_id = 'staging_dataset'
transform_dataset_id = 'transform_dataset'
reporting_dataset_id = 'reporting_dataset'
source_table = f'{project_id}.{dataset_id}.global_data'  # Main table loaded from CSV
countries = ['USA', 'India', 'Germany', 'Japan', 'France', 'Canada', 'Italy']  # Country-specific tables to be created

# DAG definition
with DAG(
    dag_id='load_create_views',
    default_args=default_args,
    description='Load CSV file from GCS to BigQuery',
    schedule =None,  # Set as required (e.g., '@daily', '0 12 * * *'), I'll not use this as I'm using a sensor to check for the file
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['bigquery', 'gcs', 'csv'],
) as dag:
    
    # Task 1: to check if the file exists in GCS
    check_file_exists = GCSObjectExistenceSensor(
        task_id='check_file_exists',
        bucket='bkt-src-global-health-data', # GCS bucket name
        object='global_health_data.csv',  # object name in the bucket
        timeout=300,  # Maximum wait time in seconds
        poke_interval=30,  # Time interval in seconds to check again
        mode='poke',  # 'poke' mode for synchronous checking
    )

    # Task 2: to load CSV from GCS to BigQuery
    load_csv_to_bigquery = GCSToBigQueryOperator(
        task_id='load_csv_to_bq',
        bucket='bkt-src-global-health-data', 
        source_objects=['global_health_data.csv'],  # data file path in the bucket
        destination_project_dataset_table='sage-extension-469317-t5.staging_dataset.global_data',  #  project, dataset, and table name
        source_format='CSV', 
        allow_jagged_rows=True,
        ignore_unknown_values=True,
        write_disposition='WRITE_TRUNCATE',  # Overwrite if the table already exists, I expect the same data structure every time
        skip_leading_rows=1,  # Skip header row
        field_delimiter=',',  # Delimiter for CSV
        autodetect=True,  # Automatically infer schema from the file
        #google_cloud_storage_conn_id='google_cloud_default',
        #bigquery_conn_id='google_cloud_default',
    )

    # Task 3: Create country-specific tables and store them in a list
    create_table_tasks = []  # List to store tasks
    create_view_tasks = []  # List to store view creation tasks
    for country in countries:
        # Task to create country-specific tables
        create_table_task = BigQueryInsertJobOperator(
            task_id=f'create_table_{country.lower()}',
            configuration={
                "query": {
                    "query": f"""
                        CREATE OR REPLACE TABLE `{project_id}.{transform_dataset_id}.{country.lower()}_table` AS
                        SELECT *
                        FROM `{source_table}`
                        WHERE country = '{country}'
                    """,
                    "useLegacySql": False,  # Use standard SQL syntax
                }
            },
        )

        # Task 4: to create view for each country-specific table with selected columns and filter
        create_view_task = BigQueryInsertJobOperator(
            task_id=f'create_view_{country.lower()}_table',
            configuration={
                "query": {
                    "query": f"""
                        CREATE OR REPLACE VIEW `{project_id}.{reporting_dataset_id}.{country.lower()}_view` AS
                        SELECT 
                            `Year` AS `year`, 
                            `Disease Name` AS `disease_name`, 
                            `Disease Category` AS `disease_category`, 
                            `Prevalence Rate` AS `prevalence_rate`, 
                            `Incidence Rate` AS `incidence_rate`
                        FROM `{project_id}.{transform_dataset_id}.{country.lower()}_table`
                        WHERE `Availability of Vaccines Treatment` = False
                    """,
                    "useLegacySql": False,
                }
            },
        )

        # Set dependencies for table creation and view creation
        create_table_task.set_upstream(load_csv_to_bigquery)
        create_view_task.set_upstream(create_table_task)
        
        create_table_tasks.append(create_table_task)  # Add table creation task to list
        create_view_tasks.append(create_view_task)  # Add view creation task to list

    # Dummy success task to run after all tables and views are created
    success_task = EmptyOperator(
        task_id='success_task',
    )

    # Define task dependencies
    check_file_exists >> load_csv_to_bigquery
    for create_table_task, create_view_task in zip(create_table_tasks, create_view_tasks):
        create_table_task >> create_view_task >> success_task
