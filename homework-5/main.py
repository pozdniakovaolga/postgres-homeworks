import json

import psycopg2

from config import config


def main():
    script_file = 'fill_db.sql'
    json_file = 'suppliers.json'
    db_name = 'new_db_hw5'

    params = config()
    conn = None

    create_database(params, db_name)
    print(f"БД {db_name} успешно создана")

    params.update({'dbname': db_name})
    try:
        with psycopg2.connect(**params) as conn:
            with conn.cursor() as cur:

                execute_sql_script(cur, script_file)
                print(f"БД {db_name} успешно заполнена")

                create_suppliers_table(cur)
                print("Таблица suppliers успешно создана")

                suppliers = get_suppliers_data(json_file)
                insert_suppliers_data(cur, suppliers)
                print("Данные в suppliers успешно добавлены")

                add_foreign_keys(cur)
                print(f"FOREIGN KEY успешно добавлены")

                conn.commit()

    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def create_database(params, db_name) -> None:
    """Создает новую базу данных."""
    conn = psycopg2.connect(dbname='postgres', **params)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(f"CREATE DATABASE {db_name}")

    cur.close()
    conn.close()


def execute_sql_script(cur, script_file) -> None:
    """Выполняет скрипт из файла для заполнения БД данными."""
    with open(script_file, encoding="UTF-8") as file:
        sql_code = file.read()
        cur.execute(sql_code)


def create_suppliers_table(cur) -> None:
    """Создает таблицу suppliers."""
    cur.execute("""
        CREATE TABLE suppliers (
            supplier_id serial PRIMARY KEY,
            company_name varchar(50),
            contact varchar(100),
            address varchar(100),
            phone varchar(50),
            fax varchar(50),
            homepage varchar(100),
            products text
        )
    """)


def get_suppliers_data(json_file: str) -> list[dict]:
    """Извлекает данные о поставщиках из JSON-файла и возвращает список словарей с соответствующей информацией."""
    with open(json_file, encoding="UTF-8") as file:
        data = json.load(file)

        return data


def insert_suppliers_data(cur, suppliers: list[dict]) -> None:
    """Добавляет данные из suppliers в таблицу suppliers."""
    for sup in suppliers:
        cur.execute(
            """
            INSERT INTO suppliers (company_name, contact, address, phone, fax, homepage, products)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (sup['company_name'], sup['contact'], sup['address'], sup['phone'], sup['fax'], sup['homepage'],
             sup['products'])
        )


def add_foreign_keys(cur) -> None:
    """Добавляет foreign key со ссылкой на supplier_id в таблицу products."""
    # добавляем колонку supplier_id в таблицу products
    cur.execute("ALTER TABLE products ADD COLUMN supplier_id int")
    # добавляем foreign key со ссылкой на supplier_id
    cur.execute(
        """
        ALTER TABLE products ADD CONSTRAINT fk_products_suppliers 
        FOREIGN KEY(supplier_id) REFERENCES suppliers(supplier_id)
        """)


if __name__ == '__main__':
    main()
