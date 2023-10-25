import pandas as pd
from sqlalchemy import create_engine, inspect, text
from decouple import config
import time


DATABASE_HOST = config("DATABASE_HOST")
DATABASE_USERNAME = config("DATABASE_USERNAME")
DATABASE_PASSWORD = config("DATABASE_PASSWORD")
DATABASE_NAME = config("DATABASE_NAME")


def measure_execution_time(func):
    """
    Декоратор для измерения времени выполнения функции.

    Args:
        func: Функция, к которой применяется декоратор.

    Returns:
        wrapper: Обертка вокруг исходной функции с измерением времени выполнения.

    """
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        formatted_time = time.strftime("%H-%M-%S", time.gmtime(execution_time))
        print(f"Время выполнения: {formatted_time}")
        return result

    return wrapper


class DatabaseAPI:
    def __init__(self, host, username, password, database_name):
        """
        Инициализация объекта DatabaseAPI.

        Args:
            host (str): Хост базы данных.\n
            username (str): Имя пользователя для подключения к базе данных.\n
            password (str): Пароль пользователя для подключения к базе данных.\n
            database_name (str): Имя базы данных.

        """
        self.host = host
        self.username = username
        self.password = password
        self.database_name = database_name
        self.engine = self.create_connection()

    def create_connection(self):
        """
        Создание соединения с базой данных.

        Returns:
            engine: Объект SQLAlchemy Engine для подключения к базе данных.

        """
        connection_url = f"postgresql+psycopg2://{self.username}:{self.password}@{self.host}/{self.database_name}"
        engine = create_engine(connection_url)
        return engine

    @measure_execution_time
    def create_table(self, table_name, dataframe):
        """
        Создание таблицы в базе данных на основе DataFrame.

        Args:
            table_name (str): Имя таблицы.\n
            dataframe (pd.DataFrame): DataFrame, который будет использован для создания таблицы.

        """
        inspector = inspect(self.engine)
        if table_name in inspector.get_table_names():
            print(f"Таблица {table_name} уже существует. Нельзя создать таблицу.")
        else:
            dataframe.to_sql(table_name, self.engine, index=False)
            print(f"Таблица {table_name} успешно создана.")

    @measure_execution_time
    def insert_sql(self, table_name, dataframe, mode):
        """
        Вставка данных в таблицу базы данных.

        Args:
            table_name (str): Имя таблицы, в которую будут вставлены данные.\n
            dataframe (pd.DataFrame): DataFrame, содержащий данные для вставки.\n
            mode (str): Режим вставки ('append' или 'replace').

        """
        inspector = inspect(self.engine)
        if table_name not in inspector.get_table_names():
            print(
                f"Таблица {table_name} не существует. Данные не могут быть добавлены."
            )
        else:
            if mode == "append":
                dataframe.to_sql(
                    table_name, self.engine, if_exists="append", index=False
                )
                print(f"Данные успешно добавлены в таблицу {table_name}.")
            elif mode == "replace":
                dataframe.to_sql(
                    table_name, self.engine, if_exists="replace", index=False
                )
                print(f"Данные успешно добавлены в таблицу {table_name}.")

    @measure_execution_time
    def truncate_table(self, table_name):
        """
        Очистка таблицы (удаление всех данных).

        Args:
            table_name (str): Имя таблицы, которую необходимо очистить.

        """
        inspector = inspect(self.engine)
        if table_name not in inspector.get_table_names():
            print(f"Таблица {table_name} не существует. Нельзя выполнить очистку.")
        else:
            with self.engine.connect() as conn:
                truncate_query = text(f"TRUNCATE TABLE {table_name}")
                conn.execute(truncate_query)
                conn.commit()
            print(f"Таблица {table_name} успешно очищена.")

    @measure_execution_time
    def read_sql(self, table_name):
        """
        Чтение данных из таблицы и возврат их в виде DataFrame.

        Args:
            table_name (str): Имя таблицы, из которой необходимо прочитать данные.

        Returns:
            pd.DataFrame: DataFrame с данными из таблицы.

        """
        inspector = inspect(self.engine)
        if table_name not in inspector.get_table_names():
            print(f"Таблица {table_name} не существует. Невозможно прочитать данные.")
            return None
        else:
            query = f"SELECT * FROM {table_name}"
            result = pd.read_sql_query(query, self.engine)
            return result

    @measure_execution_time
    def delete_from_table(self, table_name, *columns, **conditions):
        """
        Удаление данных из таблицы на основе указанных столбцов и условий.

        Args:
            table_name (str): Имя таблицы, из которой необходимо удалить данные.\n
            \*columns (str): Список столбцов, по которым выполняется условие удаления.\n
            **conditions: Условия для удаления данных.

        """
        inspector = inspect(self.engine)
        if table_name not in inspector.get_table_names():
            print(f"Таблица {table_name} не существует. Невозможно удалить данные.")
        else:
            if not columns or not conditions:
                print("Не указаны столбцы и условия для удаления.")
            else:
                with self.engine.connect() as conn:
                    delete_query = text(
                        f"DELETE FROM {table_name} WHERE "
                        + " AND ".join([f"{column} = :{column}" for column in columns])
                    )
                    conn.execute(delete_query, conditions)
                    conn.commit()
                print(f"Данные в таблице {table_name} успешно удалены.")

    @measure_execution_time
    def execute(self, query):
        """
        Выполнение произвольного SQL-запроса.

        Args:
            query (str): SQL-запрос для выполнения.

        Returns:
            result: Результат выполнения SQL-запроса.

        """
        with self.engine.connect() as conn:
            result = conn.execute(text(query))
            conn.commit()
            return result
