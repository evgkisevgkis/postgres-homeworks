"""Скрипт для заполнения данными таблиц в БД Postgres."""
import psycopg2
import csv

connection = psycopg2.connect(
    host='localhost',
    database='north',
    user='postgres',
    password='WORDPASS'
)

# заполнение таблицы с сотрудниками
with open('north_data/employees_data.csv', 'r', encoding='utf-8', newline='') as file:
    data = csv.reader(file)
    with connection:
        with connection.cursor() as cur:
            for row in data:
                if 'employee_id' in row:
                    continue
                print(row)
                cur.execute(f"INSERT INTO employees VALUES {tuple(row)}")

# заполнение таблицы с заказчиками
with open('north_data/customers_data.csv', 'r', encoding='utf-8', newline='') as file:
    data = csv.reader(file)
    with connection:
        with connection.cursor() as cur:
            for row in data:
                if 'customer_id' in row:
                    continue
                print(row)
                sql = ("INSERT INTO customers (customer_id, company_name, contact_name) VALUES (%s, %s, %s)")
                data = (row[0], row[1], row[2])
                cur.execute(sql, data)

# заполнение таблицы с заказами
with open('north_data/orders_data.csv', 'r', encoding='utf-8', newline='') as file:
    data = csv.reader(file)
    with connection:
        with connection.cursor() as cur:
            for row in data:
                if 'order_id' in row:
                    continue
                print(row)
                sql = ("INSERT INTO orders (order_id, customer_id, employee_id, order_date, ship_city)"
                       " VALUES (%s, %s, %s, %s, %s)")
                data = (row[0], row[1], row[2], row[3], row[4])
                cur.execute(sql, data)

