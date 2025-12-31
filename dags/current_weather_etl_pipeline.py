import requests
import json
from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils.dates import days_ago

# Latitude and longitude for the desired location (New York City in this case)
LATITUDE = '51.5074'
LONGITUDE = '-0.1278'
POSTGRES_CONN_ID='postgres_default'
API_CONN_ID='open_meteo_api'

default_args={
    'owner':'airflow',
    'start_date':days_ago(1)
}

## DAG

#Current Weather DAG - 15 Minute Interval

with DAG(dag_id='current_weather_etl_pipeline',
        default_args=default_args,
        schedule_interval='*/15 * * * *',
        catchup=False) as dag:
    @task()
    def extract_current_weather_data():
        """Extract weather data from Open-Meteo API using Airflow Connection."""

        # Use HTTP Hook to get connection details from Airflow connection

        http_hook=HttpHook(http_conn_id=API_CONN_ID,method='GET')

        new_endpoint = f'/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current=temperature_2m,apparent_temperature,wind_speed_10m,cloud_cover,weather_code&timezone=America%2FNew_York&temperature_unit=fahrenheit'
        
        ## Make the request via the HTTP Hook
        # response=http_hook.run(endpoint)
        response = http_hook.run(new_endpoint)

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch weather data: {response.status_code}")
        
    @task()
    def transform_current_weather_data(weather_data):
        """Transform the extracted weather data."""

        # Current Weather Data
        current_weather = weather_data['current']
        transformed_current_weather_data = {
            # 'latitude': LATITUDE,
            # 'longitude': LONGITUDE,
            'time': current_weather['time'],
            'temperature': current_weather['temperature_2m'],
            'apparent_temperature': current_weather['apparent_temperature'],
            'windspeed': current_weather['wind_speed_10m'],
            'cloud_cover': current_weather['cloud_cover'],
            'weathercode': current_weather['weather_code']
        }

        return transformed_current_weather_data
    
    @task()
    def load_current_weather_data(transformed_current_weather_data):
        """Load transformed data into PostgreSQL."""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        #CREATE TABLE
                       
        # Create Current Weather Data table if it doesn't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS current_weather_data (
            time TIMESTAMP,
            temperature FLOAT,
            apparent_temperature FLOAT,
            windspeed FLOAT,
            cloud_cover FLOAT,
            weathercode INT
        );
        """)

        # INSERT DATA 

        # Insert transformed current weather data into the table
        cursor.execute("""
        INSERT INTO current_weather_data (time, temperature, apparent_temperature, windspeed, cloud_cover, weathercode)
        VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            transformed_current_weather_data['time'],
            transformed_current_weather_data['temperature'],
            transformed_current_weather_data['apparent_temperature'],
            transformed_current_weather_data['windspeed'],
            transformed_current_weather_data['cloud_cover'],
            transformed_current_weather_data['weathercode']
        ))

       
        conn.commit()
        cursor.close()

    ## DAG Worflow- ETL Pipeline
    weather_data= extract_current_weather_data()
    transformed_current_weather_data = transform_current_weather_data(weather_data)
    load_current_weather_data(transformed_current_weather_data)

    

