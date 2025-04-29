#  Apache Spark Labs – Data Engineering Project

Лабораторная работа по дисциплине "Инструменты для больших данных".  
Цель — практическое освоение инструментов обработки, трансформации и загрузки больших данных с использованием **Apache Spark**, **PostgreSQL**, **MongoDB**, а также форматов **CSV, Avro, Parquet**.

## Описание работы

В рамках проекта разработано 5 Spark-программ, имитирующих end-to-end data pipeline:

1. **Генерация заказов в кафе и ресторанах России**  
   - Создание заказов с датами, городами, меню, ценами  
   - Вставка случайных ошибок (bit-flip, null, невалидные координаты)  
   - Выход: CSV-файл объёмом ~100 МБ

2. **Фильтрация данных и партиционирование**  
   - Фильтрация "битых" строк в deadletter-файл  
   - Запись "чистых" данных в Avro и Parquet с партиционированием по дате и городу

3. **Загрузка в PostgreSQL (3НФ)**  
   - Проектирование схемы в 3НФ  
   - Загрузка очищенных данных из Avro

4. **Загрузка в PostgreSQL (схема звезда)**  
   - Проектирование денормализованной схемы Star  
   - Загрузка из Avro

5. **Агрегация и загрузка в MongoDB**  
   - Сбор заказов в одну запись (документ)  
   - Добавление суммы и массива позиций  
   - Запись в MongoDB

## Технологии

- Apache Spark (PySpark)
- PostgreSQL
- MongoDB
- Avro, Parquet, CSV
- Python

## Структура

- **01_generate_orders.py** Signals ([link](https://github.com/mirralz/PySpark-Labs/blob/main/лабораторная_1/01_generate_orders.py)).
- **02_process_orders.py** Signal analysis ([link](https://github.com/mirralz/PySpark-Labs/blob/main/лабораторная_1/02_process_orders.py)).
- **03_load_to_postgres_3nf.py** Filters ([link](https://github.com/mirralz/PySpark-Labs/blob/main/лабораторная_1/03_load_to_postgres_3nf.py)).
- **04_load_to_postgres_star.py** Acoustic features ([link](https://github.com/mirralz/PySpark-Labs/blob/main/лабораторная_1/04_load_to_postgres_star.py)).
- **05_aggregate_to_mongodb.py** Acoustic features ([link](https://github.com/mirralz/PySpark-Labs/blob/main/лабораторная_1/05_aggregate_to_mongodb.py)).


