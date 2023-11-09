from flask import Flask, request
import os
from datetime import datetime, timezone
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor

SELECT_ALL = "SELECT * FROM "

CREATE_CAR_TABLE = "CREATE TABLE IF NOT EXISTS cars (id SERIAL PRIMARY KEY, brand TEXT);"

INSERT_CAR_TABLE = "INSERT INTO cars (brand) VALUES (%s) RETURNING id;"

CREATE_SOLD_TABLE = """CREATE TABLE IF NOT EXISTS transactions (id SERIAL PRIMARY KEY, car_id INTEGER, sale_price REAL,
                    date TIMESTAMP, FOREIGN KEY(car_id) REFERENCES cars(id) ON DELETE CASCADE);"""

INSERT_SOLD_TABLE = "INSERT INTO transactions (car_id, sale_price, date) VALUES (%s, %s, %s);"

SALES_PER_DAY = "SELECT COUNT(DISTINCT id) AS num_sales, SUM(sale_price) FROM transactions GROUP BY DATE(date);"

load_dotenv()

app = Flask(__name__)
url = os.getenv("DATABASE_URL")
conn = psycopg2.connect(url)

@app.get("/")
def home():
    return "Hello, world"

# Both get and post requests can be sent to the below route, if/else takes care of what behavior for which method
@app.route("/api/car", methods = ["GET", "POST"])
def cars():
    with conn:
        with conn.cursor(cursor_factory = RealDictCursor) as cursor: #RealDictCursor allows for the results to be returned as dict format {column : value}
            cursor.execute(CREATE_CAR_TABLE)
            # If post want to insert into the car table
            if request.method == "POST":
                data = request.get_json()
                brand = data["brand"]
                cursor.execute(INSERT_CAR_TABLE, (brand,))
                car_id = cursor.fetchone()
                return {"id" : car_id["id"], "message" : f"Entered {brand} into table."}, 201
            # If get request then want to select all and return rows
            elif request.method == "GET":
                cursor.execute(SELECT_ALL + "cars;")
                return cursor.fetchall(), 201

@app.route("/api/transaction", methods = ["GET", "POST"])
def sales():
    with conn:
        with conn.cursor(cursor_factory = RealDictCursor) as cursor:
            cursor.execute(CREATE_SOLD_TABLE)
            if request.method == "POST":
                data = request.get_json()
                car_id = data["car_id"]
                sale = data["sale_price"]
                date = datetime.now(timezone.utc)
                cursor.execute(INSERT_SOLD_TABLE, (car_id, sale, date,))
                return {"message" : f"Sold car with id {car_id} for {sale} on {date}."}, 201
            elif request.method == "GET":
                cursor.execute(SELECT_ALL + "transactions;")
                return cursor.fetchall(), 201

# Single row get method
@app.route("/api/car/<id>", methods = ["GET"])
def get_single_car(id):
    with conn:
        with conn.cursor(cursor_factory = RealDictCursor) as cursor:
            cursor.execute("SELECT * FROM cars WHERE id=" + str(id))
            return cursor.fetchone(), 201
        
# Delete single row
@app.route("/api/car/<id>", methods = ["DELETE"])
def delete_single_car(id):
    with conn:
        with conn.cursor(cursor_factory = RealDictCursor) as cursor:
            cursor.execute("DELETE FROM cars WHERE id=" + str(id))
            return f"Deleted car with id: {id}", 201

if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True, port=5000)