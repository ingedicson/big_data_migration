import os
import pandas as pd
from fastavro import writer
from sqlalchemy import create_engine
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

def backup_table_to_avro(table_name):
    backup_dir = 'backups'
    if not os.path.exists(backup_dir):
        os.makedirs(backup_dir)

    avro_file_path = os.path.join(backup_dir, f'{table_name}.avro')

    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, engine)

    if 'datetime' in df.columns:
        df['datetime'] = df['datetime'].astype(str)

    records = df.to_dict(orient='records')

    with open(avro_file_path, 'wb') as out:
        writer(out, schemas[table_name], records)

    return avro_file_path
