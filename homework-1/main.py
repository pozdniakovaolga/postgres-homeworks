# Вызываем импорты
import psycopg2
import os
import csv


# Определяем путь к файлам данных в переменные
path_to_hw1 = os.path.abspath("../homework-1/north_data/")
path_to_employees = os.path.join(path_to_hw1, "employees_data.csv")
path_to_customers = os.path.join(path_to_hw1, "customers_data.csv")
path_to_orders = os.path.join(path_to_hw1, "orders_data.csv")

# Подключаемся к базе данных
conn = psycopg2.connect(host='localhost', database='north', user='postgres', password=os.getenv('DB_PSW'))

# Заполняем таблицы данными из north_data
try:
    with conn:
        with conn.cursor() as cur:
            with open(path_to_employees, encoding="windows-1251") as csvfile:
                reader = csv.reader(csvfile, delimiter=',')
                next(reader)
                for row in reader:
                    line = tuple(row)
                    cur.execute(f'INSERT INTO employees VALUES(%s,%s,%s,%s,%s,%s)', line)
            with open(path_to_customers, encoding="windows-1251") as csvfile:
                reader = csv.reader(csvfile, delimiter=',')
                next(reader)
                for row in reader:
                    line = tuple(row)
                    cur.execute('INSERT INTO customers VALUES(%s,%s,%s)', line)
            with open(path_to_orders, encoding="windows-1251") as csvfile:
                reader = csv.reader(csvfile, delimiter=',')
                next(reader)
                for row in reader:
                    line = tuple(row)
                    cur.execute(f'INSERT INTO orders VALUES(%s,%s,%s,%s,%s)', line)
finally:
    conn.close()
