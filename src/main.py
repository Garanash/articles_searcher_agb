import os
import re
import time
import logging
import sqlite3
from threading import Thread, Lock
from datetime import datetime
from dotenv import load_dotenv
import telebot
import pandas as pd
import pytz

# Загрузка переменных окружения
load_dotenv()

# === ЛОГГИРОВАНИЕ ===
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("bot_log.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# === НАСТРОЙКИ ИЗ .env ===
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
EXCEL_FILE = 'bot_data.xlsx'  # Изменено название файла
DB_FILE = os.getenv('DB_FILE')

# Глобальная блокировка для безопасного доступа к базе данных
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


# Инициализация менеджера базы данных
db_manager = DatabaseManager(DB_FILE)


class BotWrapper:
    def __init__(self, token):
        self.token = token
        self.bot = telebot.TeleBot(token)
        self._initialize_bot()

    def _initialize_bot(self):
        """Инициализация бота с обработкой ошибок"""
        try:
            # Удаляем вебхук перед использованием polling
            self.bot.delete_webhook()
            time.sleep(1)
            self.bot.get_me()
            logger.info("Бот успешно авторизован")
            return True
        except Exception as e:
            logger.error(f"Ошибка инициализации бота: {str(e)}")
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


# Создаем экземпляр бота
bot_wrapper = BotWrapper(TELEGRAM_TOKEN)
bot = bot_wrapper.bot


def format_product_info(product):
    """Форматирование информации о продукте"""
    return (
        f"📅 Период: {product['period'] or '—'}\n"
        f"📦 Артикул: {product['article'] or '—'}\n"
        f"🏷 Наименование: {product['name'] or '—'}\n"
        f"🔢 Код: {product['code'] or '—'}\n"
        f"🏭 Склад: {product['warehouse'] or '—'}\n"
        f"📊 Остаток: {product['quantity'] or '—'}\n"
        f"💰 Цена: {product['price'] or '—'} {product['currency'] or ''}\n"
        f"📅 Дата цены: {product['price_date'] or '—'}\n"
    )


ALLOWED_USERS = {
    7513623853, 291591740, 308980455, 880161173, 7812414563, 459890220, 972172071, 747358781, 1654230, 7965375521, 7408230278, 262440194, 431233023, 913802510, 213653502, 293959414, 7426490187, 6577259391, 7825850418, 597558526
}


@bot.message_handler(commands=['start', 'help'])
def handle_start_help(message):
    if message.from_user.id not in ALLOWED_USERS:
        bot.send_message(message.chat.id, "доступ запрещен")
        return
    help_text = (
        "🔍 *Бот для поиска товаров по артикулу*\n\n"
        "Отправьте мне артикул товара — и я найду его в базе.\n"
        "Примеры:\n"
        "`805015`\n"
        "`где 805015 и 805017`\n"
    )
    bot.send_message(message.chat.id, help_text, parse_mode='Markdown')


@bot.message_handler(commands=['reload'])
def handle_reload(message):
    if message.from_user.id not in ALLOWED_USERS:
        bot.send_message(message.chat.id, "доступ запрещен")
        return
    try:
        bot.send_message(message.chat.id, "🔄 Перезагружаю базу данных...")
        success = db_manager.update_from_excel(EXCEL_FILE)
        if success:
            bot.send_message(message.chat.id, "✅ База данных успешно обновлена")
        else:
            bot.send_message(message.chat.id, "❌ Не удалось обновить базу данных")
    except Exception as e:
        logger.error(f"Ошибка при перезагрузке базы: {e}")
        bot.send_message(message.chat.id, "⚠️ Ошибка при перезагрузке базы.")


@bot.message_handler(func=lambda message: True)
@bot.message_handler(func=lambda message: True)
def handle_message(message):
    if message.from_user.id not in ALLOWED_USERS:
        bot.send_message(message.chat.id, "доступ запрещен")
        return
    try:
        user_text = message.text
        logger.info(f"Запрос от {message.from_user.id}: {user_text}")

        # Новый универсальный паттерн для артикулов: буквы, цифры, -, /, длина >= 4
        article_pattern = r"[A-Za-zА-Яа-яЁё0-9][A-Za-zА-Яа-яЁё0-9\-/]{2,}[A-Za-zА-Яа-яЁё0-9]"
        articles = re.findall(article_pattern, user_text)
        articles = list(set(articles))  # Убираем дубли

        # Фильтруем слишком короткие и неартикульные слова
        articles = [a for a in articles if len(a) >= 4 and any(c.isdigit() for c in a)]

        if not articles:
            bot.send_message(message.chat.id, "⛔️ Не найден артикул в сообщении.")
            return

        for article in articles:
            logger.info(f"Найден артикул: {article}")
            bot.send_chat_action(message.chat.id, 'typing')
            # Поиск по article (точное совпадение)
            with db_lock:
                conn = sqlite3.connect(DB_FILE)
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT * FROM products WHERE article = ? ORDER BY warehouse, period DESC
                ''', (article,))
                columns = [column[0] for column in cursor.description]
                products = [dict(zip(columns, row)) for row in cursor.fetchall()]
                conn.close()

            if not products:
                bot.send_message(message.chat.id, f"❌ Артикул {article} не найден в базе.")
                continue

            # Собираем уникальные даты установки цены
            price_dates = set(p['price_date'] for p in products if p['price_date'])
            price_dates_str = ', '.join(sorted(price_dates)) if price_dates else '—'

            # Формируем ответ
            msg = f"🔎 Артикул: {article}\n"
            msg += f"📅 Дата установки цены: {price_dates_str}\n"
            msg += f"\n"
            for product in products:
                msg += (
                    f"🏭 Склад: {product['warehouse'] or '—'}\n"
                    f"📊 Остаток: {product['quantity'] or '—'}\n"
                    f"💰 Цена: {product['price'] or '—'} {product['currency'] or ''}\n"
                    f"🏷 Наименование: {product['name'] or '—'}\n"
                    f"🔢 Код: {product['code'] or '—'}\n"
                    f"\n"
                )
            bot.send_message(message.chat.id, msg.strip())

    except Exception as e:
        logger.error(f"Ошибка при обработке сообщения: {e}")
        bot.send_message(message.chat.id, "⚠️ Ошибка при обработке запроса.")


if __name__ == "__main__":
    if bot_wrapper._initialize_bot():
        # Первоначальная загрузка базы данных

        logger.info("База данных не найдена, создаем новую...")
        db_manager.update_from_excel(EXCEL_FILE)

        logger.info("✅ Бот запущен и ждёт запросы...")
        bot_wrapper.polling()
    else:
        logger.error("❌ Бот не инициализирован. Проверьте токен.")