import os
import pandas as pd
from fastavro import reader
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)

schemas = {
    'hired_employees': {
        'type': 'record',
        'name': 'HiredEmployee',
        'fields': [
            {'name': 'id', 'type': 'int'},
            {'name': 'name', 'type': 'string'},
            {'name': 'datetime', 'type': 'string'},
            {'name': 'department_id', 'type': 'int'},
            {'name': 'job_id', 'type': 'int'}
        ]
    },
    'departments': {
        'type': 'record',
        'name': 'Department',
        'fields': [
            {'name': 'id', 'type': 'int'},
            {'name': 'department', 'type': 'string'}
        ]
    },
    'jobs': {
        'type': 'record',
        'name': 'Job',
        'fields': [
            {'name': 'id', 'type': 'int'},
            {'name': 'job', 'type': 'string'}
        ]
    }
}

def restore_table_from_avro(table_name):
    backup_dir = 'backups'
    avro_file_path = os.path.join(backup_dir, f'{table_name}.avro')

    if not os.path.exists(avro_file_path):
        raise FileNotFoundError(f"No backup found for table {table_name}")

    # Read records from the AVRO file
    with open(avro_file_path, 'rb') as f:
        avro_records = list(reader(f))

    # Convert records to a pandas DataFrame
    df = pd.DataFrame(avro_records)

    # Insert records into the database
    with engine.begin() as connection:
        for _, row in df.iterrows():
            columns = ', '.join(row.index)
            values = ', '.join([f":{col}" for col in row.index])
            insert_query = f"""
            INSERT INTO {table_name} ({columns}) 
            VALUES ({values})
            ON CONFLICT (id) DO UPDATE SET
            {', '.join([f"{col} = EXCLUDED.{col}" for col in row.index if col != 'id'])}
            """
            connection.execute(text(insert_query), row.to_dict())

    return f"Restored table {table_name} from {avro_file_path}"


if __name__ == "__main__":
    try:
        table_name = 'hired_employees' # Change this to the name of the table you want to restore
        result = restore_table_from_avro(table_name)
        print(result)
    except Exception as e:
        print(f"Error: {e}")
