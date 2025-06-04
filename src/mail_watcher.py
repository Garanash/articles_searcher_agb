import os
import imaplib
import email
import logging
import time
from threading import Thread, Lock
from email.header import decode_header
from datetime import datetime, timedelta
from dotenv import load_dotenv
import pytz
import sqlite3
import pandas as pd
import re

# Загрузка переменных окружения
load_dotenv()

# === НАСТРОЙКИ ИЗ .env ===
EMAIL = os.getenv('EMAIL')
EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD')
IMAP_SERVER = os.getenv('IMAP_SERVER')
TARGET_SENDER = os.getenv('TARGET_SENDER')
EXCEL_FILENAME = 'bot_data.xlsx'  # Изменено название файла
DB_FILE = os.getenv('DB_FILE')

# Московский часовой пояс
MOSCOW_TZ = pytz.timezone('Europe/Moscow')

# Настройка логгера
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("mail_watcher.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

db_lock = Lock()

class DatabaseManager:
    def __init__(self, db_file):
        self.db_file = db_file
        self._initialize_db()

    def _initialize_db(self):
        """Инициализация базы данных с новой структурой"""
        with db_lock:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS products (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    period TEXT,
                    article TEXT,
                    article_clean TEXT,
                    name TEXT,
                    code TEXT,
                    warehouse TEXT,
                    quantity REAL,
                    price REAL,
                    currency TEXT,
                    price_date TEXT,
                    last_updated TIMESTAMP
                )
            ''')

            cursor.execute('CREATE INDEX IF NOT EXISTS idx_article_clean ON products (article_clean)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_warehouse ON products (warehouse)')

            conn.commit()
            conn.close()

    def update_from_excel(self, excel_file):
        """Обновление базы данных из Excel файла с новой структурой"""
        if not os.path.exists(excel_file):
            logger.error(f"Файл {excel_file} не найден.")
            return False

        try:
            logger.info(f"📂 Загружаю Excel-файл {excel_file}...")
            df = pd.read_excel(excel_file)
            df = df.where(pd.notnull(df), None)

            # Нормализация артикулов
            df['article_clean'] = df['Артикул'].apply(lambda x: re.sub(r'[^\d]', '', str(x)))

            with db_lock:
                conn = sqlite3.connect(self.db_file)
                cursor = conn.cursor()

                cursor.execute('DELETE FROM products')

                for _, row in df.iterrows():
                    cursor.execute('''
                        INSERT INTO products (
                            period, article, article_clean, name, code,
                            warehouse, quantity, price, currency, price_date, last_updated
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        row.get('Период'),
                        row.get('Артикул'),
                        row['article_clean'],
                        row.get('Номенклатура'),
                        row.get('Номенклатура.Код'),
                        row.get('Склад'),
                        row.get('Остаток'),
                        row.get('Цена'),
                        row.get('Валюта'),
                        row.get('Дата установки цены'),
                        datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    ))

                conn.commit()
                conn.close()

            logger.info(f"✅ База данных успешно обновлена. Записей: {len(df)}")
            return True
        except Exception as e:
            logger.error(f"Ошибка при обновлении базы данных: {e}")
            return False

    def search_products(self, article_clean):
        """Поиск продуктов по артикулу"""
        with db_lock:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()

            cursor.execute('''
                SELECT * FROM products 
                WHERE article_clean = ?
                ORDER BY warehouse, period DESC
            ''', (article_clean,))

            columns = [column[0] for column in cursor.description]
            results = [dict(zip(columns, row)) for row in cursor.fetchall()]

            conn.close()
            return results


def decode_mail_header(header):
    """Декодирует заголовки писем"""
    if header is None:
        return ""
    decoded = decode_header(header)
    return ''.join(
        str(t[0], t[1] or 'utf-8') if isinstance(t[0], bytes) else str(t[0])
        for t in decoded
    )


def is_target_email(msg):
    """Проверяет, является ли письмо целевым"""
    from_email = msg.get('From', '')
    return TARGET_SENDER.lower() in from_email.lower()


def download_latest_excel():
    """Скачивает последний Excel-файл из целевого письма"""
    mail = None
    try:
        mail = imaplib.IMAP4_SSL(IMAP_SERVER)
        mail.login(EMAIL, EMAIL_PASSWORD)
        mail.select('INBOX')

        status, messages = mail.search(None, f'(FROM "{TARGET_SENDER}" UNSEEN)')
        if status != 'OK':
            logger.warning("Не удалось выполнить поиск писем")
            return False

        message_ids = messages[0].split()
        if not message_ids:
            logger.info("Нет новых писем от целевого отправителя")
            return False

        for msg_id in message_ids[::-1]:
            status, msg_data = mail.fetch(msg_id, '(RFC822)')
            if status != 'OK':
                continue

            msg = email.message_from_bytes(msg_data[0][1])
            if not is_target_email(msg):
                continue

            logger.info(f"Обработка письма: {decode_mail_header(msg.get('Subject', ''))}")

            for part in msg.walk():
                if part.get_content_maintype() == 'multipart':
                    continue

                filename = part.get_filename()
                if not filename:
                    continue

                filename = decode_mail_header(filename)
                if not filename.lower().endswith('.xlsx'):
                    continue

                try:
                    with open(EXCEL_FILENAME, 'wb') as f:
                        f.write(part.get_payload(decode=True))

                    logger.info(f"Файл {filename} успешно сохранен как {EXCEL_FILENAME}")
                    mail.store(msg_id, '+FLAGS', '\\Seen')
                    return True
                except Exception as e:
                    logger.error(f"Ошибка при сохранении файла: {e}")
                    continue

        logger.info("Не найдено подходящих писем с Excel-файлами")
        return False

    except Exception as e:
        logger.error(f"Ошибка: {e}")
        return False
    finally:
        if mail:
            try:
                mail.logout()
            except Exception:
                pass


def run_daily_update():
    """Запускает ежедневное обновление в 20:00 по Москве"""
    db_manager = DatabaseManager(DB_FILE)

    while True:
        try:
            now = datetime.now(MOSCOW_TZ)
            target_time = now.replace(hour=20, minute=0, second=0, microsecond=0)

            if now >= target_time:
                target_time += timedelta(days=1)

            sleep_seconds = (target_time - now).total_seconds()
            logger.info(f"Следующая проверка в {target_time.strftime('%Y-%m-%d %H:%M:%S')}")

            time.sleep(sleep_seconds)

            logger.info("Начало ежедневного обновления...")
            if download_latest_excel():
                if db_manager.update_from_excel(EXCEL_FILENAME):
                    logger.info("✅ База данных успешно обновлена")
                else:
                    logger.error("❌ Не удалось обновить базу данных")

        except Exception as e:
            logger.error(f"Ошибка в потоке обновления: {e}")
            time.sleep(3600)


if __name__ == '__main__':
    logger.info("Запуск сервиса обновления базы данных...")
    run_daily_update()