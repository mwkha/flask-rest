from flask import Flask, request
import os
from datetime import datetime, timezone
from dotenv import load_dotenv
import psycopg2

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

@app.post("/api/car")
def create_car():
    data = request.get_json()
    brand = data["brand"]
    with conn:
        with conn.cursor() as cursor:
            cursor.execute(CREATE_CAR_TABLE)
            cursor.execute(INSERT_CAR_TABLE, (brand,))
            car_id = cursor.fetchone()[0]
    return {"id" : car_id, "message" : f"Entered {brand} into table."}, 201

@app.post("/api/transaction")
def create_sale():
    data = request.get_json()
    car_id = data["car_id"]
    sale = data["sale_price"]
    date = datetime.now(timezone.utc)
    print(data)
    with conn:
        with conn.cursor() as cursor:
            cursor.execute(CREATE_SOLD_TABLE)
            cursor.execute(INSERT_SOLD_TABLE, (car_id, sale, date,))
    return {"message" : f"Sold car with id {car_id} for {sale} on {date}."}, 201


if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True, port=5000)