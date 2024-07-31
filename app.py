from flask import Flask, request, jsonify
from flask_jwt_extended import JWTManager, create_access_token, jwt_required
from sqlalchemy import create_engine, text
import pandas as pd
import logging
import re
import os
from dotenv import load_dotenv

# Secure Setup database connection and set JSON Web Tokens
app = Flask(__name__)
load_dotenv()
app.config['JWT_SECRET_KEY'] = 'bigdata_key'  
jwt = JWTManager(app)

DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)


# Set logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Secure: Endpoint to authenticate users
@app.route('/login', methods=['POST'])
def login():
    username = request.json.get('username', None)
    password = request.json.get('password', None)

    if username != 'admin' or password != 'bigdata':
        return jsonify({"msg": "Bad username or password"}), 401

    access_token = create_access_token(identity=username)
    return jsonify(access_token=access_token)

# Function to get the next value of the sequence
def get_next_id(table_name):
    query = f"SELECT nextval(pg_get_serial_sequence('{table_name}', 'id'))"
    with engine.connect() as connection:
        result = connection.execute(text(query)).fetchone()
        return result[0]

# Secure: Function to sanitize and validate data
def sanitize_and_validate_data(data, required_columns):
    sanitized_data = []
    for record in data:
        sanitized_record = {}
        for key, value in record.items():
            # Sanitize strings
            if isinstance(value, str):
                sanitized_record[key] = re.sub(r'[^\w\s]', '', value)
            else:
                sanitized_record[key] = value
        sanitized_data.append(sanitized_record)
    return sanitized_data

# Function to validate and load data
def load_data_to_db(data, table_name, required_columns):
    df = pd.DataFrame(data)
    logger.debug(f"Data received: {df}")

    # Replace empty strings with None
    df = df.replace({'': None})
    logger.debug(f"Data after replacing empty strings with None: {df}")

    # Verify rows that contain null or empty values in the required columns
    invalid_rows = df[df[required_columns].isnull().any(axis=1)]
    logger.debug(f"Invalid rows: {invalid_rows}")

    valid_rows = df.dropna(subset=required_columns)
    logger.debug(f"Valid rows: {valid_rows}")

    # If there are invalid rows, log and continue with the valid rows
    if not invalid_rows.empty:
        logger.debug(f"Returning invalid rows: {invalid_rows.to_dict(orient='records')}")
        return invalid_rows.to_dict(orient='records'), valid_rows.to_dict(orient='records')

    if valid_rows.empty:
        return [], valid_rows.to_dict(orient='records')

    # Get the next value of the sequence for each row
    valid_rows['id'] = [get_next_id(table_name) for _ in range(len(valid_rows))]
    
    logger.debug(f"Valid rows with ID: {valid_rows}")

    return [], valid_rows.to_dict(orient='records')

# Function to insert valid rows into the database
def insert_valid_rows(valid_rows, table_name):
    try:
        with engine.begin() as connection:
            for _, row in pd.DataFrame(valid_rows).iterrows():
                columns = ', '.join(row.index)
                values = ', '.join([f":{col}" for col in row.index])
                insert_query = f"""
                INSERT INTO {table_name} ({columns}) 
                VALUES ({values})
                """
                logger.debug(f"Executing query: {insert_query} with values {row.to_dict()}")
                connection.execute(text(insert_query), row.to_dict())
                logger.debug(f"Inserted row into {table_name}: {row.to_dict()}")
        return True
    except Exception as e:
        logger.error(f"Error inserting rows into {table_name}: {e}")
        return False

# Protecting the data insertion endpoint
@app.route('/insert', methods=['POST'])
@jwt_required()
def insert_data():
    try:
        json_data = request.json

        invalid_data = []
        inserted_data = []

        for table_entry in json_data:
            table = table_entry.get('table')
            data = table_entry.get('data')

            if table not in ['hired_employees', 'departments', 'jobs']:
                return jsonify({'error': 'Invalid table name'}), 400

            required_columns = {
                'hired_employees': ['name', 'datetime', 'department_id', 'job_id'],
                'departments': ['department'],
                'jobs': ['job']
            }

            # Sanitize and validate data
            sanitized_data = sanitize_and_validate_data(data, required_columns[table])
            invalid, valid = load_data_to_db(sanitized_data, table, required_columns[table])
            invalid_data.extend(invalid)

            if valid:
                if insert_valid_rows(valid, table):
                    inserted_data.extend(valid)

        response = {
            'invalid_data': invalid_data,
            'inserted_data': inserted_data
        }

        return jsonify(response), 201
    except Exception as e:
        logger.error(f"Error occurred: {e}")
        return jsonify({'error': 'Internal Server Error'}), 500

# Secure: Make sure your API is accessible only over HTTPS to protect data in transit.
if __name__ == '__main__':
    from werkzeug.middleware.proxy_fix import ProxyFix
    app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)
    app.run(host='0.0.0.0', port=5000)  # To Live set: ", ssl_context=('path/to/cert.pem', 'path/to/key.pem'))"

