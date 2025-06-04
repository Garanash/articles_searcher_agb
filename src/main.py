import telebot
import pandas as pd
import re
import os
import logging
import sqlite3
from mail_watcher import run_scheduled_check
import time
from threading import Thread, Lock
from datetime import datetime


#123

# === ЛОГГИРОВАНИЕ ===
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("bot_log.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# === НАСТРОЙКИ ===
TELEGRAM_TOKEN = '7706134881:AAFuAnYhPM1LcDNK_ZAfhwTINCX6nK34-Co'
EXCEL_FILE = 'для бота.XLSX'
DB_FILE = 'products.db'

# Глобальная блокировка для безопасного доступа к базе данных из разных потоков
db_lock = Lock()


class DatabaseManager:
    def __init__(self, db_file):
        self.db_file = db_file
        self._initialize_db()

    def _initialize_db(self):
        """Инициализация базы данных при первом запуске"""
        with db_lock:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()

            # Создаем таблицу продуктов, если она не существует
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS products (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    article TEXT,
                    article_clean TEXT,
                    article_with_spaces TEXT,
                    name TEXT,
                    code TEXT,
                    warehouse TEXT,
                    quantity TEXT,
                    price TEXT,
                    currency TEXT,
                    last_updated TIMESTAMP
                )
            ''')

            # Создаем индекс для быстрого поиска
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_article_clean ON products (article_clean)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_article_with_spaces ON products (article_with_spaces)')

            conn.commit()
            conn.close()

    def update_from_excel(self, excel_file):
        """Обновление базы данных из Excel файла"""
        if not os.path.exists(excel_file):
            logger.error(f"Файл {excel_file} не найден.")
            return False

        try:
            logger.info(f"📂 Загружаю Excel-файл {excel_file}...")
            df = pd.read_excel(excel_file, sheet_name=0)
            df = df.astype(str)

            # Нормализация артикулов
            df['Артикул_clean'] = df['Артикул'].apply(normalize)
            df['Артикул_with_spaces'] = df['Артикул'].apply(normalize_with_spaces)

            with db_lock:
                conn = sqlite3.connect(self.db_file)
                cursor = conn.cursor()

                # Очищаем таблицу перед обновлением
                cursor.execute('DELETE FROM products')

                # Вставляем новые данные
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
                        safe_get(row, 'Номенклатура'),
                        safe_get(row, 'Номенклатура.Код'),
                        safe_get(row, 'Склад'),
                        safe_get(row, 'Остаток'),
                        safe_get(row, 'Цена'),
                        safe_get(row, 'Валюта'),
                        datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    ))

                conn.commit()
                conn.close()

            logger.info(f"✅ База данных успешно обновлена. Записей: {len(df)}")
            return True
        except Exception as e:
            logger.error(f"Ошибка при обновлении базы данных: {e}")
            return False

    def search_products(self, articles_clean, articles_with_spaces):
        """Поиск продуктов в базе данных"""
        with db_lock:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()

            # Подготавливаем условия для поиска
            clean_conditions = " OR ".join(["article_clean = ?"] * len(articles_clean))
            space_conditions = " OR ".join(["article_with_spaces = ?"] * len(articles_with_spaces))

            query = f'''
                SELECT * FROM products 
                WHERE {clean_conditions or '1=0'} 
                {'OR' if clean_conditions and space_conditions else ''} 
                {space_conditions or '1=0'}
            '''

            params = articles_clean + articles_with_spaces

            cursor.execute(query, params)
            columns = [column[0] for column in cursor.description]
            results = [dict(zip(columns, row)) for row in cursor.fetchall()]

            conn.close()
            return results


# Инициализация менеджера базы данных
db_manager = DatabaseManager(DB_FILE)


class BotWrapper:
    def __init__(self, token):
        self.token = token
        self.bot = None
        self._initialize_bot()

    def _initialize_bot(self):
        """Инициализация бота с обработкой ошибок"""
        max_retries = 3
        retry_delay = 5

        for attempt in range(max_retries):
            try:
                self.bot = telebot.TeleBot(self.token)
                # Проверяем соединение
                self.bot.get_me()
                logger.info("Бот успешно авторизован")
                return True
            except Exception as e:
                logger.error(f"Попытка {attempt + 1}: Ошибка инициализации бота - {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)

        logger.critical("Не удалось инициализировать бота. Проверьте токен")
        return False

    def polling(self):
        """Запуск бота с обработкой ошибок"""
        while True:
            try:
                logger.info("Запуск бота...")
                self.bot.polling(none_stop=True, interval=3, timeout=60)
            except Exception as e:
                logger.error(f"Ошибка в работе бота: {e}")
                time.sleep(10)
                continue


# Создаем экземпляр бота
bot_wrapper = BotWrapper(TELEGRAM_TOKEN)
bot = bot_wrapper.bot if bot_wrapper.bot else None


def normalize(text):
    """Нормализация текста для простых числовых артикулов"""
    if pd.isna(text):
        return ''
    text = str(text).strip()
    text = text.replace('\xa0', '').replace('\u202f', '')
    # Для числовых артикулов оставляем только цифры
    text = re.sub(r'[^\d]', '', text)
    if text.endswith('.0'):
        text = text[:-2]
    return text.lower()


def normalize_with_spaces(text):
    """Нормализация текста с сохранением пробелов для составных артикулов"""
    if pd.isna(text):
        return ''
    text = str(text).strip()
    text = text.replace('\xa0', ' ').replace('\u202f', ' ')
    # Заменяем все нецифровые символы (кроме пробелов) на пробелы
    text = re.sub(r'[^\d\s]', ' ', text)
    # Заменяем множественные пробелы на один пробел
    text = re.sub(r'\s+', ' ', text)
    text = text.strip()
    return text.lower()


def safe_get(row, col, default='—'):
    if isinstance(row, dict):
        val = row.get(col, default)
    else:
        val = row[col] if col in row else default
    return default if str(val).lower() in ['nan', 'none', ''] else val


def find_best_matches(user_input):
    try:
        lines = user_input.split('\n')
        all_potential_compound_articles = []
        all_potential_simple_articles = []

        for line in lines:
            compound_pattern = r'\b\d{2,5}(?:\s+\d{1,5}){1,3}\b'
            potential_compound_articles = re.findall(compound_pattern, line)
            all_potential_compound_articles.extend(potential_compound_articles)

            simple_pattern = r'\b\d{4,}\b'
            potential_simple_articles = re.findall(simple_pattern, line)
            all_potential_simple_articles.extend(potential_simple_articles)

        # Нормализация и обработка артикулов
        normalized_compound = [normalize_with_spaces(c) for c in all_potential_compound_articles]
        for compound in all_potential_compound_articles:
            for part in re.findall(r'\d+', compound):
                if part in all_potential_simple_articles:
                    all_potential_simple_articles.remove(part)

        normalized_simple = [normalize(c) for c in all_potential_simple_articles]
        all_candidates = normalized_simple + normalized_compound

        if not all_candidates:
            return "⛔️ Не найден ни один артикул в сообщении."

        # Поиск в базе данных
        found_products = db_manager.search_products(normalized_simple, normalized_compound)

        if not found_products:
            return "❌ Ни один артикул не найден в базе."

        # Формирование ответа
        results = []
        found_articles = set()
        not_found_articles = []

        # Сначала обрабатываем составные артикулы
        for compound in normalized_compound:
            found = False
            for product in found_products:
                if product['article_with_spaces'] == compound:
                    found = True
                    if product['article'] not in found_articles:
                        found_articles.add(product['article'])
                        price_text = "нет" if not product['price'] or product['price'].lower() in ['nan',
                                                                                                   'none'] else f"{product['price']} {product['currency']}".strip()

                        results.append(
                            f"📦 Артикул: {product['article']}\n"
                            f"Наименование: {safe_get(product, 'name')}\n"
                            f"Код: {safe_get(product, 'code')}\n"
                            f"Склад: {safe_get(product, 'warehouse')}\n"
                            f"Остаток: {safe_get(product, 'quantity')}\n"
                            f"Цена: {price_text}\n"
                        )
            if not found:
                not_found_articles.append(compound)

        # Затем простые артикулы
        for simple in normalized_simple:
            found = False
            for product in found_products:
                if product['article_clean'] == simple:
                    found = True
                    if product['article'] not in found_articles:
                        found_articles.add(product['article'])
                        price_text = "нет" if not product['price'] or product['price'].lower() in ['nan',
                                                                                                   'none'] else f"{product['price']} {product['currency']}".strip()

                        results.append(
                            f"📦 Артикул: {product['article']}\n"
                            f"Наименование: {safe_get(product, 'name')}\n"
                            f"Код: {safe_get(product, 'code')}\n"
                            f"Склад: {safe_get(product, 'warehouse')}\n"
                            f"Остаток: {safe_get(product, 'quantity')}\n"
                            f"Цена: {price_text}\n"
                        )
            if not found:
                not_found_articles.append(simple)

        reply = "\n".join(results[:20])
        if len(results) > 20:
            reply += f"\n...и ещё {len(results) - 20} позиций"

        if not_found_articles:
            reply += f"\n\n⚠️ Не найдены в базе: {', '.join(not_found_articles)}"

        return reply
    except Exception as e:
        logger.error(f"Ошибка при поиске: {e}")
        return "⚠️ Произошла ошибка при поиске. Попробуйте позже."


@bot.message_handler(commands=['start', 'help'])
def handle_start_help(message):
    help_text = (
        "🔍 *Бот для поиска товаров по артикулу (SQLite-база)*\n\n"
        "Отправьте мне артикул товара — и я найду его в базе.\n"
        "Примеры:\n"
        "`805015`\n"
        "`где 805015 и 805017`\n"
        "`код 3546945`\n"
        "`3222 3390 07`\n"
        "`3128 0619 00`\n"
    )
    try:
        bot.send_message(message.chat.id, help_text, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Ошибка отправки сообщения: {e}")


@bot.message_handler(commands=['reload'])
def handle_reload(message):
    try:
        bot.send_message(message.chat.id, "🔄 Перезагружаю базу данных из Excel...")
        success = db_manager.update_from_excel(EXCEL_FILE)
        if success:
            bot.send_message(message.chat.id, "✅ База данных успешно обновлена")
        else:
            bot.send_message(message.chat.id, "❌ Не удалось обновить базу данных")
    except Exception as e:
        logger.error(f"Ошибка при перезагрузке базы: {e}")
        bot.send_message(message.chat.id, "⚠️ Ошибка при перезагрузке базы.")


@bot.message_handler(func=lambda message: True)
def handle_message(message):
    try:
        user_text = message.text
        logger.info(f"Запрос от {message.from_user.id}: {user_text}")
        bot.send_chat_action(message.chat.id, 'typing')
        reply = find_best_matches(user_text)

        for i in range(0, len(reply), 4000):
            try:
                bot.send_message(message.chat.id, reply[i:i + 4000])
            except Exception as e:
                logger.error(f"Ошибка отправки сообщения: {e}")
                continue

    except Exception as e:
        logger.error(f"Ошибка при обработке сообщения: {e}")
        try:
            bot.send_message(message.chat.id, "⚠️ Ошибка при обработке запроса.")
        except:
            pass


def mail_check_and_update():
    """Функция для проверки почты и обновления базы данных"""
    while True:
        try:
            # Здесь вызываем вашу функцию проверки почты
            # Предполагаем, что run_scheduled_check возвращает путь к файлу или None
            new_file = run_scheduled_check()

            if new_file and os.path.exists(new_file):
                logger.info("Обнаружен новый файл, обновляю базу данных...")
                success = db_manager.update_from_excel(new_file)
                if success:
                    logger.info("База данных успешно обновлена из почты")
                else:
                    logger.error("Не удалось обновить базу данных из почты")

            # Проверяем каждые 5 минут
            time.sleep(300)
        except Exception as e:
            logger.error(f"Ошибка в потоке проверки почты: {e}")
            time.sleep(60)


if __name__ == "__main__":
    if bot:
        # Первоначальная загрузка базы данных из Excel
        if not os.path.exists(DB_FILE):
            logger.info("База данных не найдена, создаем новую из Excel...")
            db_manager.update_from_excel(EXCEL_FILE)

        # Запускаем проверку почты в отдельном потоке
        mail_thread = Thread(target=mail_check_and_update, daemon=True)
        mail_thread.start()

        logger.info("✅ Бот запущен и ждёт запросы...")
        # Запускаем бота
        bot_wrapper.polling()
    else:
        logger.error("❌ Бот не инициализирован. Проверьте токен.")