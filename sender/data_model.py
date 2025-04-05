import json
from pathlib import Path

import psycopg2
from cv2.gapi.streaming import timestamp
from psycopg2._psycopg import cursor
from psycopg2.extras import DictCursor


class Database:
    def __init__(self):
        # Загружаем конфигурацию при инициализации класса
        self.config = self._load_configuration()

    def _load_configuration(self):
        """Загрузка конфигурации из JSON-файла"""
        try:
            config_path = Path('conf/configuration.json')
            with open(config_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Ошибка загрузки конфигурации: {e}")
            return {}

    def _connect_to_database(self):
        """Подключение к базе данных PostgreSQL"""
        try:
            conn = psycopg2.connect(
                dbname='mary',
                user="prometheus",
                password="prometheus",
                host="10.10.100.52",
                port="5432"
            )
            return conn
        except psycopg2.Error as e:
            print(f"Ошибка подключения к базе данных: {e}")
            return None

    def fetch_data_from_db(self):
        conn = self._connect_to_database()
        if conn is None:
            return []

        try:
            with conn.cursor(cursor_factory=DictCursor) as cursor:
                # Получаем start_date из конфига или используем значение по умолчанию
                start_date = self.config.get('start_date',)

                # Параметризованный запрос с правильной передачей параметров
                query = " SELECT id, file_path, timestamp FROM main.materials WHERE timestamp >= %s ORDER BY timestamp ASC LIMIT 1;"
                cursor.execute(query, (start_date,))  # Обратите внимание на запятую - создаем кортеж
                return cursor.fetchall()
        except psycopg2.Error as e:
            print(f"Ошибка получения данных: {e}")
            return []
        finally:
            conn.close()

    def insert_data_to_db(self, id, file_path, timestamp, result):
        conn = self._connect_to_database()
        if conn is None:
            return False  # Лучше возвращать bool для статуса операции

        cursor = None
        try:
            cursor = conn.cursor()
            query = """
                INSERT INTO converter_rb 
                (id, file_path, timestamp, result) 
                VALUES (%s, %s, %s, %s)
            """
            cursor.execute(query, (id, file_path, timestamp, result))
            conn.commit()  # Важно! Без этого данные не сохранятся
            return True
        except psycopg2.Error as e:
            print(f'Ошибка записи: {e}')
            conn.rollback()  # Откатываем при ошибке
            return False
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()


class Abbreviation:
    def country_abbreviation(self, country):
        if country == "rus":
            return 'RU'

