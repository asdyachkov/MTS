import csv
import cx_Oracle


def get_data_from_file(file_link: str):
    """Получение данных их файла, начиная со второй строки, т.к. значения первой (Серия;Номер)"""
    data = []
    with open(file_link, newline="") as csvfile:
        reader = csv.reader(csvfile, delimiter=" ", quotechar="|")
        for row in reader:
            data.append(row[0].split(";"))
    return data[1:]


def find_same_rows(first_file_data: list, second_file_data: list):
    """Получение одинаковых значений"""
    same_data = []
    for row in first_file_data:
        if row in second_file_data:
            same_data.append(tuple(row))
    return same_data


def append_same_data_to_oracle_db(same_data: list):
    """Добавление одинаковых значений в бд"""
    try:
        # Подключение к бд
        connection = cx_Oracle.connect("User/Password@localhost:1521/service")
        cursor = connection.cursor()

        # Проверка, существет ли бд, если нет -> создаем
        is_database_exist = cursor.execute(
            "select count(*) from ALL_TABLES where table_name='passport_data'"
        ).fetchall()
        if len(is_database_exist) < 0:
            cursor.execute(
                "create table passport_data(empid integer primary key, series varchar2(4), numbers varchar2(6))"
            )

        # Добавление всех записей в бд
        cursor.execute(
            f"insert into passport_data(series, numbers) values {', '.join(map(str, same_data))}"
        )

        print("Data added successfully")

    except Exception as e:
        print("Error with database: ", str(e))


if __name__ == "__main__":
    first_file = get_data_from_file("CSV1.csv")
    second_file = get_data_from_file("CSV2.csv")
    same_data = find_same_rows(first_file, second_file)
    append_same_data_to_oracle_db(same_data)
