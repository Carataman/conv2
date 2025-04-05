from datetime import datetime, timezone

import psycopg2
import requests
import json

from psycopg2.extras import DictCursor

from data_model import Database

import json
from datetime import datetime


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
            with conn.cursor(cursor_factory=DictCursor) as cursor:  # Используем DictCursor
                  cursor.execute(
                        "SELECT id, file_path, timestamp FROM main.materials WHERE timestamp >= '2024-10-25 10:37:12'::timestamp ORDER BY timestamp ASC LIMIT 1;", )
                  return cursor.fetchall()  # Теперь это список словарей
      except psycopg2.Error as e:
            print(f"Ошибка получения данных: {e}")
            return []
      finally:
            conn.close()


def _save_data_to_target(self, data):
      """Сохранение данных в целевую таблицу"""
      conn = self._connect_to_database()
      if conn is None:
            return False

      try:
            with conn.cursor() as cursor:
                  # Подготовка SQL для вставки
                  insert_query = """
            INSERT INTO converter_rb (id, file_path, timestamp) 
            VALUES (%s, %s, NOW())
            """

                  # Вставка всех записей
                  cursor.executemany(insert_query, data)
                  conn.commit()
                  return True
      except psycopg2.Error as e:
            conn.rollback()
            print(f"Ошибка сохранения данных: {e}")
            return False
      finally:
            conn.close()


def transfer_data(self, timestamp):
      """Основной метод для переноса данных"""
      # Получаем данные из исходной таблицы
      source_data = self._get_data_from_source(timestamp)

      if not source_data:
            print("Нет данных для переноса")
            return 0

      # Сохраняем данные в целевую таблицу
      if self._save_data_to_target(source_data):
            print(f"Успешно перенесено {len(source_data)} записей")
            return len(source_data)
      else:
            print("Ошибка при переносе данных")
            return 0

# with open("json.jpg", "rb") as file:
#       data = file.read()
#       # Находим конец JPEG-изображения (маркер 0xFFD9)
#       i = data.rfind(b"\xff\xd9")
#       if i == -1:
#             raise ValueError("Не найден конец JPEG-файла")
#
#       # Извлекаем данные после маркера
#       json_data = data[i + 2:]
#
#       # Декодируем данные
#       encodings = ['utf-8', 'windows-1251', 'latin-1']
#       result = None
#       for encoding in encodings:
#             try:
#                   result = json_data.decode(encoding).strip()
#                   break
#             except UnicodeDecodeError:
#                   continue
#
#       if result is None:
#             raise ValueError("Не удалось декодировать данные")
#
#       # Разделяем JSON-объекты
#       json_objects = result.strip().split("}{")
#
#       # Добавляем недостающие скобки
#       json_objects = [
#             "{" + obj if not obj.startswith("{") else obj
#             for obj in json_objects
#       ]
#       json_objects = [
#             obj + "}" if not obj.endswith("}") else obj
#             for obj in json_objects
#       ]
#
#       # Парсим каждый JSON-объект
#       parsed_objects = []
#       for obj in json_objects:
#             try:
#                   parsed_objects.append(json.loads(obj))
#             except json.JSONDecodeError as e:
#                   print(f"Ошибка при разборе JSON: {e}")
#                   print("Проблемный JSON-объект:")
#                   print(obj)
#                   continue
#
#       # Проверяем, что parsed_objects не пустой и содержит нужные данные
#       if not parsed_objects or len(parsed_objects) < 2:
#             raise ValueError("Недостаточно данных в JSON-объектах")
#
#       # Извлечение нужных данных
#       violation_data = parsed_objects[1].get('violation_info', {})
#       recogniser_data = parsed_objects[1].get('recogniser_info', {})
#       device_data = parsed_objects[1].get('device_info', {})
#       installation_place_data = parsed_objects[1].get('installation_place_info', {})
#
#       list_viol = {
#             "v_azimut": 0.0,
#             "v_camera": device_data.get('name_speed_meter'),
#             "v_camera_serial": device_data.get('factory_number'),
#             "v_camera_place": installation_place_data.get('place'),
#             "v_direction": "Попутное" if violation_data.get('direction') == 1 else "Встречное",
#             "v_direction_name":  installation_place_data.get('place_incoming') if installation_place_data.get('place') == "0" else installation_place_data.get('place_outcoming'),
#             "v_gps_x": float((installation_place_data.get("latitude")).strip('N')),
#             "v_gps_y": float((installation_place_data.get("longitude")).strip('E')),
#             "v_regno": recogniser_data.get('plate_chars').replace("|",""),
#             "v_regno_country_id": recogniser_data.get('plate_code'),
#             "speed": violation_data.get('speed'),
#             "v_speed_limit": violation_data.get('speed_treshold'),
#             "v_time_check": datetime.now().isoformat(),
#             "car_type": violation_data.get('type'),
#             "v_ts_model": None,
#             "v_patrol_speed": 56,
#             "v_photo_extra": "",
#             "violation": violation_data.get('crime_reason'),
#             "v_pr_viol": [recogniser_data.get('crime_reason')],
#       }
#
#     #  print(list_viol)
#
#
#
#
#
#
# try:
#     response = requests.get("http://172.21.36.63:8080/ping", timeout=5)
#     print(f"Сервер доступен. Статус: {response.status_code}")
#     if response.status_code == 200:
#           data_json = json.dumps(list_viol)
#           url = "http://172.21.36.63:8080/send"
#           headers = {'Content-Type': 'application/json'}
#           response = requests.post(url, data=data_json, headers=headers)
#           print("Статус код:", response.status_code)
#           try:
#                 json_response = response.json()
#                 print("JSON ответ:", json_response)
#           except ValueError:
#                 print("Ответ не в JSON формате:", response.text)
#
# except Exception as e:
#     print(f"Ошибка подключения: {e}")

# data_json = json.dumps(list_viol)
# url = "http://172.21.36.63:8080/send"
# headers = {'Content-Type': 'application/json'}
#
#         # Отправка запроса
# response = requests.post(url, data=data_json, headers=headers)
#         #print(f"Файл ID: {file_id}, Путь: {file_path}")
# print("Статус код:", response.status_code)
#
#         # Обработка ответа
# try:
#     json_response = response.json()
#     print("JSON ответ:", json_response)
# except ValueError:
#     print("Ответ не в JSON формате:", response.text)