from flask import Flask, jsonify, request
import mysql.connector
import logging
from cerberus import Validator
from config import DATABASE_CONFIG

logging.basicConfig(filename='app.log', level=logging.INFO,
                    format='%(asctime)s %(levelname)s: %(message)s')


status_codes = {
    200: "Success",
    400: "Bad Request",
    404: "Not Found",
    500: "Internal Server Error"
}

app = Flask(__name__)

try:
    db_config = DATABASE_CONFIG.copy()
    db_config['auth_plugin'] = 'mysql_native_password'
    db = mysql.connector.connect(**db_config)

    if db.is_connected():
        logging.info('Database connected successfully')
    else:
        logging.error('Database connection failed')
        raise Exception('Database connection failed')

except mysql.connector.Error as err:
    logging.error(f'Error connecting to the database: {err}')
    raise


data_schema = {
    'filings': {'type': 'string'},
    'descr': {'type': 'string'},
    'filed_effective': {'type': 'string'},
    'file_film_number': {'type': 'string'}
}


@app.route('/')
def home():
    return "Hello, World!"


@app.route('/api/data/', methods=['GET'])
def get_data():
    cursor = None
    try:
        cursor = db.cursor()
        cursor.execute("SELECT * FROM edgar_company_data")
        data = cursor.fetchall()
        return jsonify(data)
    except mysql.connector.Error as err:
        status_code = 500
        logging.error(f'Error fetching data: {err}')
        logging.info(f'Status {status_code}: {status_codes.get(status_code)}')
        return jsonify({"message": "Error fetching data"}), status_code
    finally:
        if cursor:
            cursor.close()


@app.route('/api/data/<int:id>', methods=['GET'])
def get_single_data(id):
    cursor = None
    try:
        cursor = db.cursor()
        query = "SELECT * FROM edgar_company_data WHERE id = %s"
        cursor.execute(query, (id,))
        data = cursor.fetchone()
        if data:
            return jsonify(data)
        else:
            status_code = 404
            logging.info(f'Status {status_code}: {status_codes.get(status_code)}')
            return jsonify({"message": "Data not found"}), status_code
    except mysql.connector.Error as err:
        status_code = 500
        logging.error(f'Error fetching data for ID {id}: {err}')
        logging.info(f'Status {status_code}: {status_codes.get(status_code)}')
        return jsonify({"message": "Error fetching data"}), status_code
    finally:
        if cursor:
            cursor.close()


@app.route('/api/data/<int:id>', methods=['DELETE'])
def delete_data(id):
    cursor = None
    try:
        cursor = db.cursor()
        query = "DELETE FROM edgar_company_data WHERE id = %s"
        cursor.execute(query, (id,))
        db.commit()
        logging.info(f'Data with ID {id} deleted successfully')
        return jsonify({"message": "Data deleted successfully"})
    except mysql.connector.Error as err:
        status_code = 500
        logging.error(f'Error deleting data with ID {id}: {err}')
        logging.info(f'Status {status_code}: {status_codes.get(status_code)}')
        return jsonify({"message": "Error deleting data"}), status_code
    finally:
        if cursor:
            cursor.close()


@app.route('/api/data/<int:id>', methods=['PUT'])
def update_data(id):
    data = request.json

    validator = Validator(data_schema)
    if not validator.validate(data):
        status_code = 400
        logging.warning(f'Invalid input data: {validator.errors}')
        logging.info(f'Status {status_code}: {status_codes.get(status_code)}')
        return jsonify({"message": "Invalid input", "errors": validator.errors}), status_code

    filings = data.get('filings')
    descr = data.get('descr')
    filed_effective = data.get('filed_effective')
    file_film_number = data.get('file_film_number')

    query = """
        UPDATE edgar_company_data
        SET filings = %s, descr = %s, filed_effective = %s, file_film_number = %s
        WHERE id = %s
    """
    values = (filings, descr, filed_effective, file_film_number, id)

    cursor = None
    try:
        cursor = db.cursor()
        cursor.execute(query, values)
        db.commit()
        logging.info(f'Data with ID {id} updated successfully')
        return jsonify({"message": "Data updated successfully"})
    except mysql.connector.Error as err:
        status_code = 500
        logging.error(f'Error updating data with ID {id}: {err}')
        logging.info(f'Status {status_code}: {status_codes.get(status_code)}')
        return jsonify({"message": "Error updating data"}), status_code
    finally:
        if cursor:
            cursor.close()


if __name__ == '__main__':
    app.run(debug=True)
