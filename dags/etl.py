from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from datetime import datetime, timedelta
import json


with DAG(
    dag_id='project1_nasa',
    start_date=datetime(2025, 1, 1),
    schedule='@daily',
    catchup=False
) as dag:

    ## Step 1: Create a table if it doesn't exist
    @task
    def create_table():
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')

        create_table_query = '''
            CREATE TABLE IF NOT EXISTS apod_data (
                id SERIAL PRIMARY KEY,
                title VARCHAR(255),
                explanation TEXT,
                url TEXT,
                date DATE,
                media_type VARCHAR(50)
            )
        '''

        postgres_hook.run(create_table_query)

    ## Step 2: Get APOD data from NASA API (Extract)
    extract_apod = HttpOperator(
        task_id="extract_apod",
        http_conn_id="nasa_api",
        endpoint="planetary/apod",
        method="GET",
        data={"api_key": "{{ conn.nasa_api.extra_dejson.api_key }}"},
        response_filter=lambda response: response.json(),
        log_response=True
    )

    ## Step 3: Transform the data
    @task
    def transform_apod_data(response):
        apod_data = {
            'title': response.get('title', ''),
            'explanation': response.get('explanation', ''),
            'url': response.get('url', ''),
            'date': response.get('date', ''),
            'media_type': response.get('media_type', '')
        }
        return apod_data

    ## Step 4: Load the data into Postgres database
    @task
    def load_data_to_postgres(apod_data):
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')

        insert_query = """
        INSERT INTO apod_data (title, explanation, url, date, media_type)
        VALUES (%s, %s, %s, %s, %s);
        """

        postgres_hook.run(
            insert_query,
            parameters=(
                apod_data['title'],
                apod_data['explanation'],
                apod_data['url'],
                apod_data['date'],
                apod_data['media_type']
            )
        )

    ## Step 5: Establish dependencies
    create_table_task = create_table()
    extracted_response = extract_apod.output
    transformed_data = transform_apod_data(extracted_response)
    
    create_table_task >> extract_apod
    transformed_data >> load_data_to_postgres(transformed_data)