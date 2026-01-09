from airflow import DAG
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import sqlite3
from datetime import datetime

# Function to read data from SQLite and write to CSV
def read_sqlite_and_write_to_csv():
    # Connect to the SQLite database
    conn = sqlite3.connect('/home/kiwilytics/Desktop/RetailDB/RetailDB.sqlite')  # Replace with your SQLite DB path
    
    # Query to fetch data (modify based on your table and data)
    query = "SELECT * FROM categories;"  # Replace with your actual query
    
    # Use pandas to read from SQLite into a DataFrame
    df = pd.read_sql(query, conn)
    
    # Write the DataFrame to CSV
    df.to_csv('/home/kiwilytics/Desktop/RetailDB/retail_data.csv', index=False)  # Provide your desired path
    
    # Close the connection
    conn.close()

# Default arguments for the DAG
default_args = {
    'owner': 'kiwilytics',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 29),  # Change to your preferred start date
    'retries': 0,
}

# Define the DAG
with DAG(
    'sqlite_to_csv_dag',
    default_args=default_args,
    description='A simple DAG that reads from SQLite and writes to CSV',
    schedule_interval=None,  # Set to None for manual trigger or set a cron expression
    catchup=False,
) as dag:

    # Task to read from SQLite and write to CSV
    read_and_write_task = PythonOperator(
        task_id='read_from_sqlite_and_write_to_csv',
        python_callable=read_sqlite_and_write_to_csv,
    )

    read_and_write_task
