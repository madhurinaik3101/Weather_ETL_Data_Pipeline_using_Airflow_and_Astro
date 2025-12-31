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

# Hourly Weather DAG - Once every Hour

with DAG(dag_id='hourly_weather_etl_pipeline',
        default_args=default_args,
        schedule_interval='@hourly',
        catchup=False) as dag:
    @task()
    def extract_hourly_weather_data():      
        """Extract weather data from Open-Meteo API using Airflow Connection."""

        # Use HTTP Hook to get connection details from Airflow connection

        http_hook=HttpHook(http_conn_id=API_CONN_ID,method='GET')

        new_endpoint = f'/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&hourly=temperature_2m,apparent_temperature,precipitation_probability,cloud_cover,wind_speed_10m,uv_index,sunshine_duration,weather_code&timezone=America%2FNew_York&temperature_unit=fahrenheit'
        
        ## Make the request via the HTTP Hook
        # response=http_hook.run(endpoint)
        response = http_hook.run(new_endpoint)

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch weather data: {response.status_code}")
        
    @task()
    def transform_hourly_weather_data(weather_data):
        """Transform the extracted weather data."""

       # Hourly Weather Data for 7 Days
        hourly_weather = weather_data['hourly']
        transformed_hourly_weather_data = {
            'time': hourly_weather['time'],
            'temperature_2m': hourly_weather['temperature_2m'],
            'apparent_temperature': hourly_weather['apparent_temperature'],
            'precipitation_probability': hourly_weather['precipitation_probability'],
            'cloud_cover': hourly_weather['cloud_cover'],
            'wind_speed_10m': hourly_weather['wind_speed_10m'],
            'uv_index': hourly_weather['uv_index'],
            'sunshine_duration': hourly_weather['sunshine_duration'],
            'weathercode': hourly_weather['weather_code']
        }

        return transformed_hourly_weather_data
    
    @task()
    def load_hourly_weather_data(transformed_hourly_weather_data):
        """Load transformed data into PostgreSQL."""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Create Hourly Weather Data table if it doesn't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS hourly_weather_data (
            time TIMESTAMP,
            temperature FLOAT,
            apparent_temperature FLOAT,
            precipitation_probability FLOAT,
            cloud_cover FLOAT,
            wind_speed FLOAT,
            uv_index FLOAT,
            sunshine_duration FLOAT,
            weathercode INT
        );
        """)

        cursor.execute("TRUNCATE TABLE hourly_weather_data;")

        #INSERT DATA
        
            # Insert transformed hourly weather data into the table
            # cursor.execute("""
            # INSERT INTO hourly_weather_data (time, temperature, apparent_temperature, precipitation_probability, cloud_cover, wind_speed, uv_index, sunshine_duration, weathercode)
            # VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            # """, (
            #     transformed_hourly_weather_data['time'][i],
            #     transformed_hourly_weather_data['temperature_2m'][i],
            #     transformed_hourly_weather_data['apparent_temperature'][i],
            #     transformed_hourly_weather_data['precipitation_probability'][i],
            #     transformed_hourly_weather_data['cloud_cover'][i],
            #     transformed_hourly_weather_data['wind_speed_10m'][i],
            #     transformed_hourly_weather_data['uv_index'][i],
            #     transformed_hourly_weather_data['sunshine_duration'][i],
            #     transformed_hourly_weather_data['weathercode'][i]
            # ))

        cursor.execute("""
        INSERT INTO hourly_weather_data (
            time, temperature, apparent_temperature, 
            precipitation_probability, cloud_cover, 
            wind_speed, uv_index, sunshine_duration, weathercode
        )
        SELECT 
            UNNEST(%s::timestamp[]),
            UNNEST(%s::float[]),
            UNNEST(%s::float[]),
            UNNEST(%s::float[]),
            UNNEST(%s::float[]),
            UNNEST(%s::float[]),
            UNNEST(%s::float[]),
            UNNEST(%s::float[]),
            UNNEST(%s::int[])
        """, (
        transformed_hourly_weather_data['time'],
        transformed_hourly_weather_data['temperature_2m'],
        transformed_hourly_weather_data['apparent_temperature'],
        transformed_hourly_weather_data['precipitation_probability'],
        transformed_hourly_weather_data['cloud_cover'],
        transformed_hourly_weather_data['wind_speed_10m'],
        transformed_hourly_weather_data['uv_index'],
        transformed_hourly_weather_data['sunshine_duration'],
        transformed_hourly_weather_data['weathercode']
        ))

        conn.commit()
        cursor.close()
    
    ## DAG Worflow- ETL Pipeline
    weather_data= extract_hourly_weather_data()
    transformed_hourly_weather_data = transform_hourly_weather_data(weather_data)
    load_hourly_weather_data(transformed_hourly_weather_data)