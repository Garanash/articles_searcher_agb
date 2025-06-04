import imaplib
import email
import os
import logging
import shutil
import tempfile
from email.header import decode_header
import time
import threading
from datetime import datetime, timedelta
import pytz
from contextlib import contextmanager
import sqlite3

# === НАСТРОЙКИ ===
EMAIL = 'almazgeobur.it@mail.ru'
PASSWORD = 'K7cAiTCjvVn50YiHqdnp'
IMAP_SERVER = 'imap.mail.ru'
TARGET_SENDER = '1c@almazgeobur.kz'
DB_FILE = 'products.db'
EXCEL_FILENAME = 'для бота.xlsx'

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

# Московский часовой пояс
MOSCOW_TZ = pytz.timezone('Europe/Moscow')


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
    """Проверяет, является ли письмо целевым (от нужного отправителя)"""
    from_email = msg.get('From', '')
    return TARGET_SENDER.lower() in from_email.lower()


@contextmanager
def atomic_file_replace(filename):
    """Контекстный менеджер для атомарной замены файла"""
    temp_dir = tempfile.gettempdir()
    temp_path = os.path.join(temp_dir, f"temp_{os.path.basename(filename)}")
    backup_path = os.path.join(temp_dir, f"backup_{os.path.basename(filename)}")

    try:
        if os.path.exists(filename):
            shutil.copy2(filename, backup_path)

        yield temp_path

        if os.path.exists(temp_path):
            if os.path.exists(filename):
                os.unlink(filename)
            shutil.move(temp_path, filename)

    except Exception as e:
        logger.error(f"Ошибка при замене файла: {e}")
        if os.path.exists(backup_path) and not os.path.exists(filename):
            shutil.copy2(backup_path, filename)
        raise
    finally:
        for path in [temp_path, backup_path]:
            try:
                if os.path.exists(path):
                    os.unlink(path)
            except Exception:
                pass


def update_database_from_excel(excel_file):
    """Обновляет базу данных из Excel файла"""
    if not os.path.exists(excel_file):
        logger.error(f"Файл {excel_file} не найден.")
        return False

    try:
        logger.info(f"Обновление базы данных из файла {excel_file}...")

        # Подключение к базе данных
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()

        # Чтение Excel файла
        df = pd.read_excel(excel_file, sheet_name=0)
        df = df.astype(str)

        # Нормализация артикулов
        df['Артикул_clean'] = df['Артикул'].apply(lambda x: re.sub(r'[^\d]', '', str(x)))
        df['Артикул_with_spaces'] = df['Артикул'].apply(
            lambda x: ' '.join(re.findall(r'\d+', str(x)))
        )

        # Очистка таблицы перед обновлением
        cursor.execute('DELETE FROM products')

        # Вставка новых данных
        for _, row in df.iterrows():
            cursor.execute('''
                INSERT INTO products (
                    article, article_clean, article_with_spaces, name, code, 
                    warehouse, quantity, price, currency, last_updated
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                row['Артикул'],
                row['Артикул_clean'],
                row['Артикул_with_spaces'],
                row.get('Номенклатура', ''),
                row.get('Номенклатура.Код', ''),
                row.get('Склад', ''),
                row.get('Остаток', ''),
                row.get('Цена', ''),
                row.get('Валюта', ''),
                datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            ))

        conn.commit()
        logger.info(f"База данных успешно обновлена. Записей: {len(df)}")
        return True

    except Exception as e:
        logger.error(f"Ошибка при обновлении базы данных: {e}")
        return False
    finally:
        if conn:
            conn.close()


def download_latest_excel():
    """Скачивает последний Excel-файл из целевого письма"""
    mail = None
    try:
        mail = imaplib.IMAP4_SSL(IMAP_SERVER)
        mail.login(EMAIL, PASSWORD)
        mail.select(MAILBOX)

        # Ищем непрочитанные письма от целевого отправителя
        status, messages = mail.search(None, f'(FROM "{TARGET_SENDER}" UNSEEN)')
        if status != 'OK':
            logger.warning("Не удалось выполнить поиск писем")
            return False

        message_ids = messages[0].split()
        if not message_ids:
            logger.info("Нет новых писем от целевого отправителя")
            return False

        for msg_id in message_ids[::-1]:  # Обрабатываем от новых к старым
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
                    with atomic_file_replace(EXCEL_FILENAME) as temp_file:
                        with open(temp_file, 'wb') as f:
                            f.write(part.get_payload(decode=True))

                    logger.info(f"Файл {filename} успешно сохранен как {EXCEL_FILENAME}")

                    # Помечаем письмо как прочитанное
                    mail.store(msg_id, '+FLAGS', '\\Seen')

                    return True
                except Exception as e:
                    logger.error(f"Ошибка при сохранении файла: {e}")
                    continue

        logger.info("Не найдено подходящих писем с Excel-файлами")
        return False

    except imaplib.IMAP4.error as e:
        logger.error(f"Ошибка IMAP: {e}")
        return False
    except Exception as e:
        logger.error(f"Неожиданная ошибка: {e}")
        return False
    finally:
        if mail:
            try:
                mail.logout()
            except Exception:
                pass


def calculate_next_run_time():
    """Вычисляет время следующего запуска в 20:00 по Москве"""
    now = datetime.now(MOSCOW_TZ)
    target_time = now.replace(hour=20, minute=0, second=0, microsecond=0)

    # Если сегодняшнее время уже прошло, планируем на завтра
    if now >= target_time:
        target_time += timedelta(days=1)

    return target_time


def run_daily_check():
    """Запускает ежедневную проверку почты в 20:00 по Москве"""
    while True:
        try:
            next_run = calculate_next_run_time()
            now = datetime.now(MOSCOW_TZ)

            sleep_seconds = (next_run - now).total_seconds()
            logger.info(
                f"Следующая проверка почты в {next_run.strftime('%Y-%m-%d %H:%M:%S')} (через {sleep_seconds / 3600:.1f} часов)")

            time.sleep(sleep_seconds)

            logger.info("Начало ежедневной проверки почты...")
            if download_latest_excel():
                if update_database_from_excel(EXCEL_FILENAME):
                    logger.info("База данных успешно обновлена из почты")
                else:
                    logger.error("Не удалось обновить базу данных из почты")

        except Exception as e:
            logger.error(f"Ошибка в потоке проверки почты: {e}")
            time.sleep(3600)  # Ждем час при ошибке


if __name__ == '__main__':
    # Запускаем проверку почты в отдельном потоке
    mail_thread = threading.Thread(target=run_daily_check, daemon=True)
    mail_thread.start()

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("Остановка почтового watcher'а")