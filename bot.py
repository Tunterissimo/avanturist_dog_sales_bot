import time
import logging
import os
from contextlib import contextmanager
from functools import lru_cache
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
    CallbackQueryHandler,
)
import gspread
from google.oauth2.service_account import Credentials
import psycopg2
from psycopg2.extras import RealDictCursor
import json
from datetime import datetime

# ==================== КОНФИГУРАЦИЯ ====================
BOT_TOKEN = os.environ["BOT_TOKEN"]
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
DATABASE_URL = os.environ["DATABASE_URL"]
CREDENTIALS_JSON = os.environ["CREDENTIALS"]

# Константы для кеширования
CACHE_TTL_SECONDS = 300  # 5 минут

# Константы для Google Sheets
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
SHEET_NAME = 'Тест'
PRODUCT_SHEET_NAME = 'Продукция'

# Список каналов продаж
SALES_CHANNELS = ["Сайт", "Инстаграм", "Телеграм", "Озон", "Маркеты"]

# Ожидаемые заголовки колонок в таблице продаж
EXPECTED_HEADERS = ["Канал", "Товар", "Количество", "Цена", "Сумма", "Дата"]

# ==================== НАСТРОЙКА ЛОГГИРОВАНИЯ ====================
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("bot.log"),
    ],
)
logger = logging.getLogger(__name__)

# ==================== БАЗА ДАННЫХ ====================
@contextmanager
def get_db_connection():
    """Контекстный менеджер для подключений к БД. Автоматически закрывает соединение."""
    conn = None
    try:
        conn = psycopg2.connect(DATABASE_URL, sslmode="require")
        logger.debug("✅ Успешное подключение к БД")
        yield conn
    except Exception as e:
        logger.error(f"❌ Ошибка подключения к БД: {e}")
        raise
    finally:
        if conn:
            conn.close()

@contextmanager
def get_db_cursor():
    """Контекстный менеджер для курсора. Автоматически закрывает и курсор, и соединение."""
    with get_db_connection() as conn:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        try:
            yield cur
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cur.close()

def init_db():
    """Инициализация таблицы в БД"""
    try:
        with get_db_cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS user_states (
                    user_id BIGINT PRIMARY KEY,
                    channel VARCHAR(50),
                    product_id VARCHAR(20),
                    product_name VARCHAR(100),
                    product_price VARCHAR(20),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
        logger.info("✅ База данных инициализирована успешно")
    except Exception as e:
        logger.error(f"❌ Ошибка инициализации БД: {e}")

# ==================== GOOGLE SHEETS ====================
# Парсим JSON credentials
credentials_info = json.loads(CREDENTIALS_JSON)

@lru_cache(maxsize=1)
def get_google_sheet_cached():
    """Получает лист Google Sheets с кешированием"""
    try:
        logger.info("🔄 Инициализирую новое подключение к Google Sheets...")
        creds = Credentials.from_service_account_info(credentials_info, scopes=SCOPES)
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(SPREADSHEET_ID)
        worksheet = spreadsheet.worksheet(SHEET_NAME)
        
        logger.info("✅ Новое подключение к Google Sheets установлено")
        return worksheet
    except Exception as e:
        logger.error(f"❌ Ошибка инициализации Google Sheets: {e}")
        raise

@lru_cache(maxsize=1)
def get_products_from_sheet():
    """Загружает список товаров из Google Таблицы с кешированием"""
    try:
        logger.info("🔄 Загружаю список товаров из Google Таблицы...")
        sheet = get_google_sheet_cached()

        try:
            product_sheet = sheet.spreadsheet.worksheet(PRODUCT_SHEET_NAME)
            logger.info("✅ Лист 'Продукция' найден")
        except Exception as e:
            logger.error(f"❌ Лист 'Продукция' не найден: {e}")
            return []

        all_data = product_sheet.get_all_values()
        logger.info(f"📊 Получено строк с листа 'Продукция': {len(all_data)}")

        # Пропускаем заголовок
        products_data = all_data[1:] if len(all_data) > 1 else []
        
        # Формируем список товаров
        product_list = []
        for row in products_data:
            if len(row) >= 3 and row[0] and row[1]:
                product_list.append({
                    'id': row[0].strip(),
                    'name': row[1].strip(),
                    'price': row[2].strip() if len(row) >= 3 and row[2] else '0'
                })
        
        logger.info(f"✅ Загружено {len(product_list)} товаров")
        return product_list
        
    except Exception as e:
        logger.error(f"❌ Ошибка загрузки товаров: {e}")
        return []

def get_product_price(product_id):
    """Получает цену товара по его ID"""
    try:
        products = get_products_from_sheet()
        product = next((p for p in products if p['id'] == str(product_id)), None)
        
        if product and 'price' in product:
            return float(product['price'])
        else:
            logger.error(f"❌ Цена для товара {product_id} не найдена")
            return None
    except Exception as e:
        logger.error(f"❌ Ошибка получения цены: {e}")
        return None

def products_keyboard():
    """Создает клавиатуру с товарами"""
    try:
        products = get_products_from_sheet()
        keyboard = []
        
        # Создаем кнопки (по 2 в ряд)
        for i in range(0, len(products), 2):
            row = []
            row.append(InlineKeyboardButton(products[i]['name'], callback_data=f"product_{products[i]['id']}"))
            
            if i + 1 < len(products):
                row.append(InlineKeyboardButton(products[i+1]['name'], callback_data=f"product_{products[i+1]['id']}"))
            
            keyboard.append(row)
        
        # Добавляем кнопку "Отмена"
        keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel")])
        
        return InlineKeyboardMarkup(keyboard)
    except Exception as e:
        logger.error(f"❌ Ошибка создания клавиатуры товаров: {e}")
        # Возвращаем пустую клавиатуру с кнопкой отмены
        return InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="cancel")]])

def sales_channels_keyboard():
    """Создает клавиатуру с каналами продаж"""
    keyboard = []
    for i in range(0, len(SALES_CHANNELS), 2):
        row = [
            InlineKeyboardButton(SALES_CHANNELS[i], callback_data=SALES_CHANNELS[i]),
            InlineKeyboardButton(SALES_CHANNELS[i + 1], callback_data=SALES_CHANNELS[i + 1])
            if i + 1 < len(SALES_CHANNELS)
            else None,
        ]
        row = [btn for btn in row if btn is not None]
        keyboard.append(row)
    return InlineKeyboardMarkup(keyboard)

# ==================== ОБРАБОТЧИКИ КОМАНД ====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start"""
    user_name = update.message.from_user.first_name
    help_text = f"""
Привет, {user_name}! Я бот для учета продаж.

Чтобы добавить новую запись, используй команду /add
Для генерации отчета используй команду /report
"""
    await update.message.reply_text(help_text, parse_mode="Markdown")

async def add_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /add"""
    user_id = update.message.from_user.id

    # Очищаем предыдущее состояние пользователя в БД
    try:
        with get_db_cursor() as cur:
            cur.execute(
                "INSERT INTO user_states (user_id) VALUES (%s) ON CONFLICT (user_id) DO UPDATE SET channel = NULL, product_id = NULL, product_name = NULL, product_price = NULL",
                (user_id,),
            )
    except Exception as e:
        logger.error(f"❌ Ошибка БД в add_entry: {e}")
        await update.message.reply_text("❌ Ошибка сервиса. Попробуйте позже.")
        return

    # Спрашиваем канал продаж с клавиатурой
    await update.message.reply_text(
        "Выберите канал продаж:", reply_markup=sales_channels_keyboard()
    )

# ==================== ОБРАБОТЧИКИ КНОПОК ====================
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик нажатий на инлайн-кнопки"""
    query = update.callback_query
    user_id = query.from_user.id
    data = query.data
    
    await query.answer()
    
    # 1. Обработка ВЫБОРА ТОВАРА
    if data.startswith("product_"):
        try:
            product_id = data.split("_")[1]
            products = get_products_from_sheet()
            selected_product = next((p for p in products if p['id'] == product_id), None)
            
            if selected_product:
                with get_db_cursor() as cur:
                    cur.execute(
                        "UPDATE user_states SET product_id = %s, product_name = %s, product_price = %s WHERE user_id = %s",
                        (selected_product['id'], selected_product['name'], selected_product['price'], user_id)
                    )
                
                await query.edit_message_text(text=f"✅ Выбран товар: {selected_product['name']}")
                await query.message.reply_text("Теперь введите только количество:\n\nПример: `2`", parse_mode='Markdown')
            else:
                await query.edit_message_text(text="❌ Товар не найден")
                await query.message.reply_text("Попробуйте выбрать товар еще раз:", reply_markup=products_keyboard())
                
        except Exception as e:
            logger.error(f"❌ Ошибка БД при выборе товара: {e}")
            await query.message.reply_text("❌ Ошибка сервиса. Попробуйте снова.")
    
    # 2. Обработка ОТМЕНЫ
    elif data == "cancel":
        try:
            with get_db_cursor() as cur:
                cur.execute(
                    "UPDATE user_states SET channel = NULL, product_id = NULL, product_name = NULL, product_price = NULL WHERE user_id = %s",
                    (user_id,)
                )
            await query.edit_message_text(text="❌ Операция отменена")
        except Exception as e:
            logger.error(f"❌ Ошибка БД при отмене: {e}")
            await query.edit_message_text(text="❌ Операция отменена")
    
    # 3. Обработка ВЫБОРА КАНАЛА ПРОДАЖ
    elif data in SALES_CHANNELS:
        try:
            with get_db_cursor() as cur:
                cur.execute(
                    "UPDATE user_states SET channel = %s, product_id = NULL, product_name = NULL, product_price = NULL WHERE user_id = %s",
                    (data, user_id)
                )
            
            await query.edit_message_text(text=f"✅ Выбран канал: {data}")
            
            # Загружаем товары и показываем клавиатуру
            try:
                products = get_products_from_sheet()
                if products:
                    await query.message.reply_text(
                        "Выберите товар:",
                        reply_markup=products_keyboard()
                    )
                else:
                    await query.message.reply_text("❌ Нет доступных товаров. Обратитесь к администратору.")
            except Exception as e:
                logger.error(f"❌ Не удалось загрузить товары: {e}")
                await query.message.reply_text("❌ Не удалось загрузить каталог товаров. Попробуйте позже.")
                
        except Exception as e:
            logger.error(f"❌ Ошибка БД при выборе канала: {e}")
            await query.answer("❌ Ошибка сохранения. Попробуйте снова.")
    
    # 4. Обработка ОТЧЕТОВ
    elif data.startswith("report_"):
        await handle_report_buttons(update, context)
    
    # 5. Если callback_data не распознан
    else:
        logger.warning(f"⚠️ Неизвестный callback_data: {data}")
        await query.edit_message_text(text="❌ Неизвестная команда")

# ==================== ОБРАБОТЧИК ТЕКСТОВЫХ СООБЩЕНИЙ ====================
async def handle_product_data(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик ввода количества товара"""
    user_message = update.message.text
    user_id = update.message.from_user.id
    
    # Достаем состояние пользователя из БД
    try:
        with get_db_cursor() as cur:
            cur.execute("SELECT channel, product_id, product_name, product_price FROM user_states WHERE user_id = %s", (user_id,))
            user_state = cur.fetchone()

        if not user_state or not user_state["channel"]:
            await update.message.reply_text("❌ Сначала выберите канал продаж через команду /add")
            return
            
        if not user_state["product_name"]:
            await update.message.reply_text("❌ Сначала выберите товар через команду /add")
            return

        channel = user_state["channel"]
        product_name = user_state["product_name"]
        product_price = float(user_state["product_price"]) if user_state["product_price"] else 0

    except Exception as e:
        logger.error(f"❌ Ошибка БД в handle_product_data: {e}")
        await update.message.reply_text("❌ Ошибка сервиса. Попробуйте позже.")
        return

    # Принимаем ТОЛЬКО количество
    try:
        quantity_input = user_message.strip()
        quantity = float(quantity_input.replace(",", "."))

        if quantity <= 0:
            await update.message.reply_text("❌ Количество должно быть больше 0")
            return

        # Записываем в таблицу
        sheet = get_google_sheet_cached()

        # Получаем все данные для поиска первой пустой строки
        all_data = sheet.get_all_values()
        next_row = len(all_data) + 1  # Следующая после последней заполненной

        # Подготавливаем данные для вставки
        row_data = [channel, product_name, quantity, product_price, quantity * product_price, datetime.now().strftime("%d.%m.%Y %H:%M")]
        
        # Вставляем данные
        sheet.append_row(row_data)
        logger.info(f"✅ Данные записаны в строку {next_row}: {row_data}")

        # Формируем сообщение об успехе
        success_text = f"""✅ Данные успешно добавлены!

*Канал продаж:* {channel}
*Товар:* {product_name}
*Количество:* {quantity}
*Цена:* {product_price:.2f} руб.
*Сумма:* {quantity * product_price:.2f} руб.
"""
        await update.message.reply_text(success_text, parse_mode="Markdown")
        logger.info(f"👤 User {user_id} added record: {row_data}")

    except ValueError:
        await update.message.reply_text(
            "❌ Ошибка: количество должно быть числом. Пример: `2` или `1.5`",
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.error(f"❌ Ошибка при записи в Google Таблицу: {e}", exc_info=True)
        error_msg = "❌ Произошла ошибка при записи в Google Таблицу. Попробуйте позже."
        await update.message.reply_text(error_msg)

# ==================== ОТЧЕТЫ ====================
async def generate_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Генерация отчета"""
    try:
        keyboard = [
            [InlineKeyboardButton("📊 Отчет по каналам продаж", callback_data="report_channels")],
            [InlineKeyboardButton("📦 Отчет по товарам", callback_data="report_products")],
            [InlineKeyboardButton("❌ Отмена", callback_data="report_cancel")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            "📈 Выберите тип отчета:",
            reply_markup=reply_markup
        )
        
    except Exception as e:
        logger.error(f"❌ Ошибка в generate_report: {e}")
        await update.message.reply_text("❌ Ошибка генерации отчета")

async def handle_report_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик кнопок отчетов"""
    query = update.callback_query
    data = query.data
    
    await query.answer()
    
    if data == "report_channels":
        await generate_channels_report(query)
    elif data == "report_products":
        await generate_products_report(query)
    elif data == "report_cancel":
        await query.edit_message_text("❌ Генерация отчета отменена")

async def generate_channels_report(query):
    """Генерация отчета по каналам продаж"""
    try:
        sheet = get_google_sheet_cached()
        all_data = sheet.get_all_values()
        
        if len(all_data) <= 1:
            await query.edit_message_text("📊 Нет данных для отчета")
            return
        
        # Находим индексы колонок по заголовкам
        headers = all_data[0]
        try:
            channel_idx = headers.index("Канал")
            product_idx = headers.index("Товар")
            qty_idx = headers.index("Количество")
            price_idx = headers.index("Цена")
            amount_idx = headers.index("Сумма")
        except ValueError as e:
            logger.error(f"❌ Не найдена ожидаемая колонка: {e}. Заголовки: {headers}")
            await query.edit_message_text("❌ Ошибка: таблица имеет неверную структуру.")
            return
        
        # Парсим данные
        sales_data = []
        for row in all_data[1:]:
            # Пропускаем пустые строки
            if not any(row):
                continue
                
            if len(row) > max(channel_idx, product_idx, qty_idx, amount_idx) and row[channel_idx] and row[qty_idx] and row[amount_idx]:
                try:
                    sales_data.append({
                        'channel': row[channel_idx],
                        'product': row[product_idx] if len(row) > product_idx else '',
                        'quantity': float(row[qty_idx].replace(',', '.')),
                        'price': float(row[price_idx].replace(',', '.')) if len(row) > price_idx and row[price_idx] else 0,
                        'amount': float(row[amount_idx].replace(',', '.'))
                    })
                except ValueError:
                    continue
        
        if not sales_data:
            await query.edit_message_text("📊 Нет данных для анализа")
            return
        
        # Анализируем данные по каналам
        channel_stats = {}
        for sale in sales_data:
            channel = sale['channel']
            if channel not in channel_stats:
                channel_stats[channel] = {
                    'count': 0,
                    'total_amount': 0,
                    'total_quantity': 0
                }
            
            channel_stats[channel]['count'] += 1
            channel_stats[channel]['total_amount'] += sale['amount']
            channel_stats[channel]['total_quantity'] += sale['quantity']
        
        # Формируем отчет
        report_text = "📊 ОТЧЕТ ПО КАНАЛАМ ПРОДАЖ\n\n"
        report_text += "```\n"
        report_text += "Канал         | Продажи | Количество | Сумма       \n"
        report_text += "--------------+---------+------------+-------------\n"
        
        total_amount = 0
        total_quantity = 0
        total_sales = len(sales_data)
        
        for channel, stats in sorted(channel_stats.items()):
            report_text += f"{channel:<13} | {stats['count']:>7} | {stats['total_quantity']:>10.2f} | {stats['total_amount']:>11.2f}\n"
            total_amount += stats['total_amount']
            total_quantity += stats['total_quantity']
        
        report_text += "--------------+---------+------------+-------------\n"
        report_text += f"{'ИТОГО':<13} | {total_sales:>7} | {total_quantity:>10.2f} | {total_amount:>11.2f}\n"
        report_text += "```"
        
        await query.edit_message_text(report_text, parse_mode="Markdown")
        
    except Exception as e:
        logger.error(f"❌ Ошибка генерации отчета: {e}")
        await query.edit_message_text("❌ Ошибка генерации отчета")

async def generate_products_report(query):
    """Генерация отчета по товарам"""
    try:
        sheet = get_google_sheet_cached()
        all_data = sheet.get_all_values()
        
        if len(all_data) <= 1:
            await query.edit_message_text("📦 Нет данных для отчета")
            return
        
        # Находим индексы колонок по заголовкам
        headers = all_data[0]
        try:
            product_idx = headers.index("Товар")
            qty_idx = headers.index("Количество")
            amount_idx = headers.index("Сумма")
        except ValueError as e:
            logger.error(f"❌ Не найдена ожидаемая колонка: {e}. Заголовки: {headers}")
            await query.edit_message_text("❌ Ошибка: таблица имеет неверную структуру.")
            return
        
        # Анализируем данные по товарам
        product_stats = {}
        for row in all_data[1:]:
            # Пропускаем пустые строки
            if not any(row):
                continue
                
            if len(row) > max(product_idx, qty_idx, amount_idx) and row[product_idx] and row[qty_idx] and row[amount_idx]:
                try:
                    product = row[product_idx]
                    quantity = float(row[qty_idx].replace(',', '.'))
                    amount = float(row[amount_idx].replace(',', '.'))
                    
                    if product not in product_stats:
                        product_stats[product] = {
                            'count': 0,
                            'total_amount': 0,
                            'total_quantity': 0
                        }
                    
                    product_stats[product]['count'] += 1
                    product_stats[product]['total_amount'] += amount
                    product_stats[product]['total_quantity'] += quantity
                    
                except ValueError:
                    continue
        
        if not product_stats:
            await query.edit_message_text("📦 Нет данных для анализа")
            return
        
        # Формируем отчет
        report_text = "📦 ОТЧЕТ ПО ТОВАРАМ\n\n"
        report_text += "```\n"
        report_text += "Товар               | Продажи | Количество | Сумма       \n"
        report_text += "--------------------+---------+------------+-------------\n"
        
        for product, stats in sorted(product_stats.items()):
            report_text += f"{product:<19} | {stats['count']:>7} | {stats['total_quantity']:>10.2f} | {stats['total_amount']:>11.2f}\n"
        
        report_text += "```"
        
        await query.edit_message_text(report_text, parse_mode="Markdown")
        
    except Exception as e:
        logger.error(f"❌ Ошибка генерации отчета по товарам: {e}")
        await query.edit_message_text("❌ Ошибка генерации отчета")

# ==================== ЗАПУСК БОТА ====================
if __name__ == "__main__":
    # Инициализируем базу данных при старте
    init_db()

    # Создаем и запускаем бота
    application = Application.builder().token(BOT_TOKEN).build()

    # Добавляем обработчики
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("add", add_entry))
    application.add_handler(CommandHandler("report", generate_report))
    application.add_handler(CallbackQueryHandler(button_handler))
    application.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, handle_product_data)
    )

    # Запускаем бота
    logger.info("🤖 Бот запущен...")
    application.run_polling()