from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import os
import pandas as pd
import random

default_args = {
    'owner': 'dataquest',
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

output_dir = '/opt/airflow/tmp'
raw_file = 'raw_events.csv'
transformed_file = 'transformed_events.csv'
raw_path = os.path.join(output_dir, raw_file)
transformed_path = os.path.join(output_dir, transformed_file)

def generate_events():
    """
    Simulate extracting event data from an external source.
    In a real pipeline, this might hit an API, scrape a page,
    or read from a database. Here we generate synthetic data
    so the pipeline is fully self-contained for testing.
    """
    events = [
        "Solar flare disrupts satellite",
        "New open-weight AI model released",
        "Fusion reactor hits net energy gain",
        "Lunar eclipse visible tonight",
        "Central bank adjusts interest rate",
        "Flooding reported in Nairobi",
        "Quantum computing breakthrough at CERN",
        "Artemis crew lands on the Moon",
        "Earthquake tremors detected in Tokyo",
        "Global shift to open-source accelerates",
    ]
    sample = random.sample(events, 5)
    data = {
        "timestamp": [
            datetime.now().strftime("%Y-%m-%d %H:%M:%S") for _ in sample
        ],
        "event": sample,
        "intensity": [round(random.uniform(1, 10), 2) for _ in sample],
        "category": [
            random.choice(["Science", "Tech", "Weather", "Space", "Finance"])
            for _ in sample
        ],
    }
    df = pd.DataFrame(data)
    os.makedirs(output_dir, exist_ok=True)
    df.to_csv(raw_path, index=False)
    print(f"Generated {len(df)} events → {raw_path}")

def transform_events():
    """
    Read the raw extract, apply transformations, and write a
    clean output file.
    """
    df = pd.read_csv(raw_path)
    df_sorted = df.sort_values(by="intensity", ascending=False)
    df_sorted.to_csv(transformed_path, index=False)
    print(f"Transformed {len(df_sorted)} events → {transformed_path}")

# -------------------------------------------------------------------
# Task 3 will go here in our next lesson:
# We'll add an upload_to_gcs() function that sends the transformed
# CSV to a Google Cloud Storage bucket. This requires:
#   - The google-cloud-storage Python library
#   - A GCS bucket
#   - A service account with write permissions
# All of that gets configured when we connect to GCS.
# -------------------------------------------------------------------

with DAG(
    dag_id="etl_events_pipeline",
    default_args=default_args,
    description="Generate and transform event data daily",
    start_date=datetime(2026, 5, 18),
    schedule="@daily",
    catchup=False,
) as dag:

    extract = PythonOperator(
        task_id="generate_events",
        python_callable=generate_events,
    )

    transform = PythonOperator(
        task_id="transform_events",
        python_callable=transform_events,
    )

    # In our next lesson, we'll add: upload = PythonOperator(...)
    # and extend the chain to: extract >> transform >> upload

    extract >> transform