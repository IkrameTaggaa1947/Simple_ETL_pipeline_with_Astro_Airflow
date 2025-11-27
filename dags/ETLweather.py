from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from datetime import datetime, timedelta


LATITUDE = 51.5074
LONGITUDE = -0.1278
POST_GRES_CONN_ID = "postgres_default"
API_CONN_ID = "open_meteo_api"


default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 11, 24),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DAG

with DAG(
    dag_id="etl_weather_pipeline",
    default_args=default_args,
    schedule="@daily",
    catchup=False
) as dag:

    @task()
    def extract_weather_data():
        http_hook = HttpHook(http_conn_id=API_CONN_ID, method="GET")
        endpoint = f"/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true"
        # https://api.open-meteo.com/v1/forecast?latitude=51.5074&longitude=-0.1278&current_weather=true
        response = http_hook.run(endpoint)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch data: {response.status_code}")

    @task()
    def transform_weather_data(weather_data: dict):
        current_weather = weather_data.get('current_weather', {})
        transformed_data = {
            'latitude': LATITUDE,
            'longitude': LONGITUDE,
            'temperature': current_weather.get("temperature"),
            'windspeed': current_weather.get("windspeed"),
            'winddirection': current_weather.get("winddirection"),
            'weathercode': current_weather.get("weathercode"),
        }
        return transformed_data

    @task()
    def load_weather_data(transformed_data: dict):
        pg_hook = PostgresHook(postgres_conn_id=POST_GRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        # Create table if not exists
        cursor.execute("""
           CREATE TABLE IF NOT EXISTS weather_data (
               id SERIAL PRIMARY KEY,
               latitude FLOAT,
               longitude FLOAT,
               temperature FLOAT,
               windspeed FLOAT,
               winddirection FLOAT,
               weathercode INT,
               recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
           );
       """)
        # Pushing Weather Data
        cursor.execute("""
       INSERT INTO weather_data (latitude, longitude, temperature, windspeed, winddirection, weathercode)
       VALUES (%s, %s, %s, %s, %s, %s)
       """, (
            transformed_data['latitude'],
            transformed_data['longitude'],
            transformed_data['temperature'],
            transformed_data['windspeed'],
            transformed_data['winddirection'],
            transformed_data['weathercode']
        ))
        conn.commit()
        cursor.close()

    # DAG Workflow - ETL Pipeline
    weather_data = extract_weather_data()
    transformed_data = transform_weather_data(weather_data)
    load_weather_data(transformed_data)
