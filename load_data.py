from flask import Flask, request, jsonify
from sqlalchemy import create_engine, text
import pandas as pd
import logging
import os
from dotenv import load_dotenv

# Create logs folder if don't exist
log_dir = 'logs'
log_file = os.path.join(log_dir, 'data_migration.log')

print("Starting script...")

if not os.path.exists(log_dir):
    print(f"Creating log directory: {log_dir}")
    os.makedirs(log_dir)

# Setup logging
print(f"Setting up logging to {log_file}")
logging.basicConfig(filename=log_file, level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.info('Start Processing')


# Setup database connection
load_dotenv()
app = Flask(__name__)

DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)



def load_csv_to_db(file_path, table_name, required_columns):
    print(f"Loading data from {file_path} to table {table_name}")
    df = pd.read_csv(file_path)
    
    # Verify rows that contain null values in the required columns
    invalid_rows = df[df[required_columns].isnull().any(axis=1)]
    
    # Log invalid rows
    for _, row in invalid_rows.iterrows():
        logger.info(f"Invalidate row, no inserted {table_name}: {row.to_dict()}")
    
    # Filter invalid rows
    valid_rows = df.dropna(subset=required_columns)
    
    # If there are no valid rows, exit the function
    if valid_rows.empty:
        print(f"No valid rows to insert into {table_name}")
        return

    # Insert valid rows into the database
    valid_rows.to_sql(table_name, con=engine, if_exists='append', index=False)
    print(f"Inserted valid rows into {table_name}")

def adjust_sequence(table_name):
    query = f"SELECT setval(pg_get_serial_sequence('{table_name}', 'id'), COALESCE((SELECT MAX(id) FROM {table_name}) + 1, 1), FALSE)"
    with engine.connect() as connection:
        connection.execute(text(query))
    print(f"Adjusted sequence for {table_name}")

# Load and move data from CSV
load_csv_to_db('data/hired_employees.csv', 'hired_employees', ['name', 'datetime', 'department_id', 'job_id'])
load_csv_to_db('data/departments.csv', 'departments', ['id', 'department'])
load_csv_to_db('data/jobs.csv', 'jobs', ['id', 'job'])

# Adjust sequences Id after data insertion
adjust_sequence('hired_employees')
adjust_sequence('departments')
adjust_sequence('jobs')

print("Script completed.")
