from api import DatabaseAPI  # Импортируем наш класс DatabaseAPI
from decouple import config
import pandas as pd

# Получаем настройки подключения к базе данных из конфигурационных файлов
DATABASE_HOST = config("DATABASE_HOST")
DATABASE_USERNAME = config("DATABASE_USERNAME")
DATABASE_PASSWORD = config("DATABASE_PASSWORD")
DATABASE_NAME = config("DATABASE_NAME")

# Инициализируем объект DatabaseAPI для взаимодействия с базой данных
db = DatabaseAPI(
    host=DATABASE_HOST,
    username=DATABASE_USERNAME,
    password=DATABASE_PASSWORD,
    database_name=DATABASE_NAME
)

# Создаем DataFrame с данными
df = pd.DataFrame({
    "column1": ["value1", "value2", "value3"],
    "column2": ["value1", "value2", "value3"]
})

# Создание таблицы и вставка данных
db.create_table('sample_table', df)
db.insert_sql('sample_table', df, 'replace')

# Чтение данных из таблицы
result = db.read_sql('sample_table')
print("Данные из таблицы 'sample_table':")
print(result)

# Очистка таблицы
db.truncate_table('sample_table')

# Выполнение произвольного SQL-запроса
query = "SELECT * FROM sample_table"
result = db.execute(query)
print("Результат SQL-запроса:")
for row in result:
    print(row)

# Удаление данных из таблицы с одним условием
table_name = "sample_table"
columns = ["column1"]
conditions = {"column1": "value1"}
db.delete_from_table(table_name, *columns, **conditions)
print(f"Удалены данные из таблицы '{table_name}' где 'column1' = 'value1'")

# Удаление данных из таблицы с несколькими условиями
table_name = "sample_table"
columns = ["column1", "column2"]
conditions = {"column1": "value1", "column2": "value2"}
db.delete_from_table(table_name, *columns, **conditions)
print(f"Удалены данные из таблицы '{table_name}' где 'column1' = 'value1' и 'column2' = 'value2'")