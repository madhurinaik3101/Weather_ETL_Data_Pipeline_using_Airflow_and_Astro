import requests
import json
from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils.dates import days_ago

# Latitude and longitude for the desired location (New York City in this case)
LATITUDE = '40.730610'
LONGITUDE = '-73.935242'
POSTGRES_CONN_ID='postgres_default'
API_CONN_ID='open_meteo_api'

default_args={
    'owner':'airflow',
    'start_date':days_ago(1)
}

## DAG

# Daily Weather DAG - Once in 12 Hours

with DAG(dag_id='daily_weather_etl_pipeline',
        default_args=default_args,
        schedule_interval='0 */12 * * *',
        catchup=False) as dag:
    @task()
    def extract_daily_weather_data():
        """Extract weather data from Open-Meteo API using Airflow Connection."""        
        
        # Use HTTP Hook to get connection details from Airflow connection

        http_hook=HttpHook(http_conn_id=API_CONN_ID,method='GET')

        new_endpoint = f'/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&daily=apparent_temperature_max,apparent_temperature_min,sunrise,sunset,sunshine_duration,wind_speed_10m_max&hourly=temperature_2m,apparent_temperature,precipitation_probability,cloud_cover,wind_speed_10m,uv_index,sunshine_duration,weather_code&current=temperature_2m,apparent_temperature,wind_speed_10m,cloud_cover,weather_code&timezone=America%2FNew_York&temperature_unit=fahrenheit'
       
        ## Make the request via the HTTP Hook
        # response=http_hook.run(endpoint)
        response = http_hook.run(new_endpoint)

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch weather data: {response.status_code}")
    
    @task()
    def transform_daily_weather_data(weather_data):
        """Transform the extracted weather data."""

        # Daily Data for 7 Days
        daily_weather = weather_data['daily']
        transformed_daily_weather_data = {
            'day': daily_weather['time'],
            'apparent_temperature_max': daily_weather['apparent_temperature_max'],
            'apparent_temperature_min': daily_weather['apparent_temperature_min'],
            'sunrise': daily_weather['sunrise'],
            'sunset': daily_weather['sunset'],
            'sunshine_duration': daily_weather['sunshine_duration'],
            'wind_speed': daily_weather['wind_speed_10m_max']
        }

        return transformed_daily_weather_data
    
    @task()
    def load_daily_weather_data(transformed_daily_weather_data):
        """Load transformed data into PostgreSQL."""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        #CREATE TABLES

        #Create Daily Weather Data Table if not exists
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS daily_weather_data (
                day DATE,
                apparent_temperature_max FLOAT,
                apparent_temperature_min FLOAT,
                sunrise TIMESTAMP,
                sunset TIMESTAMP,
                sunshine_duration FLOAT,
                wind_speed FLOAT
        );
        """)

        cursor.execute("TRUNCATE TABLE daily_weather_data;")

        #INSERT DATA

        cursor.execute("""
        INSERT INTO daily_weather_data (
        day, apparent_temperature_max, apparent_temperature_min,
        sunrise, sunset, sunshine_duration, wind_speed
                     )
        SELECT 
            UNNEST(%s::date[]),
            UNNEST(%s::float[]),
            UNNEST(%s::float[]),
            UNNEST(%s::timestamp[]),
            UNNEST(%s::timestamp[]),
            UNNEST(%s::float[]),
            UNNEST(%s::float[])
        """, (
            transformed_daily_weather_data['day'],
            transformed_daily_weather_data['apparent_temperature_max'],
            transformed_daily_weather_data['apparent_temperature_min'],
            transformed_daily_weather_data['sunrise'],
            transformed_daily_weather_data['sunset'],
            transformed_daily_weather_data['sunshine_duration'],
            transformed_daily_weather_data['wind_speed']
        ))

        conn.commit()
        cursor.close()

        # Insert transformed daily weather data into the table
        # cursor.execute ("""
        # INSERT INTO daily_weather_data (day, apparent_temperature_max, apparent_temperature_min, sunrise, sunset, sunshine_duration, wind_speed)
        # VALUES (%s, %s, %s, %s, %s, %s, %s)
        # """, (
        #     transformed_daily_weather_data['day'],
        #     transformed_daily_weather_data['apparent_temperature_max'],
        #     transformed_daily_weather_data['apparent_temperature_min'],
        #     transformed_daily_weather_data['sunrise'],
        #     transformed_daily_weather_data['sunset'],
        #     transformed_daily_weather_data['sunshine_duration'],
        #     transformed_daily_weather_data['wind_speed']
        # ))

    ## DAG Worflow- ETL Pipeline
    weather_data= extract_daily_weather_data()
    transformed_daily_weather_data = transform_daily_weather_data(weather_data)
    load_daily_weather_data(transformed_daily_weather_data)