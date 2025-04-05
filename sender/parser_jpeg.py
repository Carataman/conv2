import json
import os
import re
from datetime import datetime, timezone, timedelta
from ftplib import FTP
from io import BytesIO
from typing import Optional, Dict, List, Any
import requests
import logging
from dataclasses import dataclass
import data_model
import base64

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class ViolationData:
    """Класс для хранения данных о нарушении"""
    v_azimut: float = 0.0
    v_camera: Optional[str] = None
    v_camera_serial: Optional[str] = None
    v_camera_place: Optional[str] = None
    v_direction: str = ""
    v_direction_name: str = ""
    v_gps_x: float = 0.0
    v_gps_y: float = 0.0
    v_regno: str = ""
    v_regno_country_id: Optional[str] = None
    speed: Optional[int] = None
    v_speed_limit: Optional[int] = None
    v_time_check: str = ""
    car_type: Optional[str] = None
    v_ts_model: Optional[str] = None
    v_patrol_speed: int = 56
    v_photo_extra: str = ""
    violation: Optional[str] = None
    v_pr_viol: List[str] = None
    v_photo_ts: str = ""
    v_photo_extra: List[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {k: v for k, v in self.__dict__.items()}


class ParserJpeg:
    def __init__(self, ftp_host: str = '10.10.100.52',
                 ftp_user: str = 'ftp3',
                 ftp_pass: str = 'ftp3',
                 api_url: str = 'http://172.21.36.63:8080'):
        """
        Инициализация парсера JPEG-файлов с FTP-сервера
        """
        self.ftp_host = ftp_host
        self.ftp_user = ftp_user
        self.ftp_pass = ftp_pass
        self.api_url = api_url
        self.db = data_model.Database()
        self.data = self.fetch_data()

    def fetch_data(self) -> List[tuple]:
        """Получает список файлов для обработки из базы данных"""
        try:
            rows = self.db.fetch_data_from_db()
            return [(row['id'], row["file_path"], row['timestamp']) for row in rows]
        except Exception as e:
            logger.error(f"Ошибка при извлечении данных: {e}")
            return []

    def _download_jpg_ftp(self, file_path: str) -> Optional[bytes]:
        """Загружает файл с FTP в память"""
        ftp = None
        try:
            ftp = FTP(self.ftp_host)
            ftp.login(user=self.ftp_user, passwd=self.ftp_pass)

            remote_path = file_path.replace("/mnt/targets/ftp/all_fixations", '/')
            dirname = os.path.dirname(remote_path)
            filename = os.path.basename(remote_path)

            try:
                ftp.cwd(dirname)
            except Exception as e:
                logger.error(f"Директория {dirname} не найдена: {e}")
                return None

            if filename not in ftp.nlst():
                logger.warning(f"Файл {filename} не найден в {dirname}")
                return None

            with BytesIO() as file_data:
                ftp.retrbinary(f'RETR {filename}', file_data.write)
                logger.info(f"Успешно загружен файл {filename} ({len(file_data.getvalue())} байт)")
                return file_data.getvalue()

        except Exception as e:
            logger.error(f"FTP ошибка: {e}")
            return None
        finally:
            if ftp:
                ftp.quit()

    def _parse_coordinate(self, coord_str: str) -> float:
        """Парсит координату из строки"""
        if not coord_str:
            return 0.0

        try:
            # Удаляем все символы, кроме цифр, точки и знака минуса
            clean_str = re.sub(r"[^\d.-]", "", coord_str)
            return float(clean_str)
        except (ValueError, TypeError):
            return 0.0



    def _parse_jpg(self, jpeg_data: bytes) -> Optional[ViolationData]:
        """Парсит JPEG-файл и извлекает данные с изображениями в base64"""
        try:
            # Извлекаем все JPEG изображения в base64
            jpeg_frames = self._extract_jpeg_frames(jpeg_data)
            if not jpeg_frames:
                raise ValueError("No JPEG frames found in data")

            # Создаем объект нарушения
            violation = ViolationData()

            # Первое изображение - основное фото
            violation.v_photo_ts = jpeg_frames[0]
            # Остальные изображения - дополнительные фото
            violation.v_photo_extra = jpeg_frames[1:] if len(jpeg_frames) > 1 else []

            # Извлекаем JSON данные после последнего изображения
            json_data = self._extract_json_data(jpeg_data)
            if not json_data:
                raise ValueError("No JSON data found after JPEG frames")

            # Парсим JSON данные
            parsed_objects = self._parse_json_data(json_data)
            if not parsed_objects:
                raise ValueError("No valid JSON objects found")

            # Заполняем остальные поля данными из JSON
            self._fill_violation_data(violation, parsed_objects[-1])

            return violation

        except Exception as e:
            logger.error(f"Error parsing JPEG data: {e}")
            return None

    def _extract_jpeg_frames(self, data: bytes) -> List[str]:
        """Извлекает все JPEG изображения и конвертирует в base64"""
        frames = []
        pos = 0
        start_marker = b'\xff\xd8'
        end_marker = b'\xff\xd9'

        while pos < len(data):
            # Ищем начало JPEG
            start_pos = data.find(start_marker, pos)
            if start_pos == -1:
                break

            # Ищем конец JPEG
            end_pos = data.find(end_marker, start_pos)
            if end_pos == -1:
                break

            # Извлекаем кадр (включая маркер конца)
            frame = data[start_pos:end_pos + 2]
            # Конвертируем в base64
            frames.append(base64.b64encode(frame).decode('utf-8'))

            # Перемещаем позицию
            pos = end_pos + 2

        return frames

    def _extract_json_data(self, data: bytes) -> Optional[bytes]:
        """Извлекает JSON данные после последнего JPEG изображения"""
        # Находим конец последнего JPEG
        last_jpeg_end = data.rfind(b'\xff\xd9')
        if last_jpeg_end == -1:
            return None

        # JSON данные идут после последнего JPEG
        return data[last_jpeg_end + 2:]

    def _parse_json_data(self, json_data: bytes) -> List[dict]:
        """Парсит JSON данные с обработкой ошибок"""
        # Декодируем с разными кодировками
        json_str = None
        for encoding in ['utf-8', 'windows-1251', 'latin-1']:
            try:
                json_str = json_data.decode(encoding).strip()
                break
            except UnicodeDecodeError:
                continue

        if not json_str:
            raise ValueError("Failed to decode JSON data")

        # Пробуем распарсить как единый JSON
        try:
            parsed_data = json.loads(json_str)
            return [parsed_data] if not isinstance(parsed_data, list) else parsed_data
        except json.JSONDecodeError:
            pass

        # Если не получилось, пробуем разделить вручную
        json_objects = []
        buffer = ""
        brace_count = 0

        for char in json_str:
            buffer += char
            if char == '{':
                brace_count += 1
            elif char == '}':
                brace_count -= 1
                if brace_count == 0:
                    try:
                        json_objects.append(json.loads(buffer))
                    except json.JSONDecodeError as e:
                        logger.warning(f"Partial JSON parse error: {e}")
                    buffer = ""

        return json_objects

    def _fill_violation_data(self, violation: ViolationData, data: dict):
        """Заполняет объект ViolationData данными из JSON"""
        device_info = data.get('device_info', {})
        installation_place = data.get('installation_place_info', {})
        violation_info = data.get('violation_info', {})
        recogniser_info = data.get('recogniser_info', {})

        # Заполнение полей
        violation.v_camera = device_info.get('name_speed_meter') #название прибора
        violation.v_camera_serial = device_info.get('factory_number') #серийный номер прибора
        violation.v_camera_place = installation_place.get('place') #место установки
        violation.v_direction = "Попутное" if installation_place.get('direction')  == 0 else "Встречное" #направление
        violation.v_direction_name = installation_place.get('place_outcoming"') if installation_place.get('direction')  == 1 else installation_place.get('place_outcoming')
        # Обработка координат
        lat = installation_place.get("latitude", "0")
        lon = installation_place.get("longitude", "0")
        violation.v_gps_x = float(re.sub(r"[^\d.-]", "", lat)) #широта
        violation.v_gps_y = float(re.sub(r"[^\d.-]", "", lon)) #долгота

        # Обработка времени
        utc_timestamp = violation_info.get('UTC')
        if utc_timestamp:
            dt = datetime.utcfromtimestamp(utc_timestamp)
            milliseconds = violation_info.get('ms', 0)
            timezone_offset = violation_info.get('timezone', 0) * 360
            dt += timedelta(milliseconds=milliseconds, hours=timezone_offset // 3600)
            violation.v_time_check = dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] #время фиксации
        else:
            violation.v_time_check = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]

        # Остальные поля
        violation.v_regno = recogniser_info.get('plate_chars', '').replace("|", "") #GRZ
        violation.v_regno_country_id = recogniser_info.get('plate_code') #region
        violation.v_speed = violation_info.get('speed') #Speed
        violation.v_speed_limit = violation_info.get('speed_threshold') # скорость фиксации нарушения
        violation.v_car_type = violation_info.get('type') #тип тс
        violation.v_patrol_speed = violation_info.get('self_speed') #скорость патруля
        violation.v_ts_model = f"({recogniser_info.get('mark')}/{recogniser_info.get('model')})" #марка модель
        violation.v_pr_viol = [violation_info.get('crime_reason')]

    def _send_to_server(self, data: Dict[str, Any]) -> bool:
        """Отправляет данные на сервер"""
        try:
            response = requests.get(f"{self.api_url}/ping", timeout=5)
            if response.status_code != 200:
                logger.error(f"Сервер недоступен. Статус: {response.status_code}")
                return False

            response = requests.post(
                f"{self.api_url}/send",
                json=data,
                headers={'Content-Type': 'application/json'},
                timeout=10
            )

            if response.status_code == 200:
                try:
                    logger.info(f"Успешная отправка. Ответ: {response.json()}")
                    data_model.Database.insert_data_to_db(data['id'], data['file_path'], data['timestamp'], response.status_code)
                except ValueError:
                    logger.info(f"Успешная отправка. Ответ: {response.text}")
                return True
            else:
                logger.error(f"Ошибка отправки. Статус: {response.status_code}")
                return False

        except Exception as e:
            logger.error(f"Ошибка соединения с сервером: {e}")
            return False

    def process_all(self) -> None:
        """Обрабатывает все нарушения и отправляет на сервер"""
        success_count = 0
        for file_id, file_path, _ in self.data:
            logger.info(f"Обработка файла {file_path} (ID: {file_id})")

            jpeg_data = self._download_jpg_ftp(file_path)
            if not jpeg_data:
                continue

            violation = self._parse_jpg(jpeg_data)
            if not violation:
                continue

            if self._send_to_server(violation.to_dict()):
                success_count += 1

        logger.info(f"Обработка завершена. Успешно отправлено: {success_count}/{len(self.data)}")




if __name__ == "__main__":
    parser = ParserJpeg()
    parser.process_all()

