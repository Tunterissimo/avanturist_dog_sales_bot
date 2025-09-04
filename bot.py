import time 
import logging
import os
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

gsheet_client = None
gsheet_worksheet = None
gsheet_last_init = 0

def get_google_sheet_cached():
    global gsheet_client, gsheet_worksheet, gsheet_last_init
    
    # Переиспользуем подключение в течение 5 минут
    if (gsheet_client is not None and gsheet_worksheet is not None and 
        time.time() - gsheet_last_init < 300):  # 300 секунд = 5 минут
        logger.info("✅ Использую кешированное подключение к Google Sheets")
        return gsheet_worksheet
    
    # Инициализируем новое подключение
    try:
        logger.info("Инициализирую новое подключение к Google Sheets...")
        creds = Credentials.from_service_account_info(credentials_info, scopes=SCOPES)
        gsheet_client = gspread.authorize(creds)
        spreadsheet = gsheet_client.open_by_key(SPREADSHEET_ID)
        gsheet_worksheet = spreadsheet.worksheet('Тест')
        gsheet_last_init = time.time()
        
        logger.info("✅ Новое подключение к Google Sheets установлено")
        return gsheet_worksheet
        
    except Exception as e:
        logger.error(f"❌ Ошибка инициализации Google Sheets: {e}")
        raise

product_list = []
product_last_update = 0

def get_products_from_sheet():
    global product_list, product_last_update
    
    # Обновляем список раз в 5 минут
    if product_list and time.time() - product_last_update < 300:
        logger.info("✅ Использую кешированный список товаров")
        return product_list
    
    try:
        logger.info("Загружаю список товаров из Google Таблицы...")
        sheet = get_google_sheet_cached()

        try:
            product_sheet = sheet.spreadsheet.worksheet('Продукция')  # Открываем лист "Продукция"
            logger.info("✅ Лист 'Продукция' найден")
        except Exception as e:
            logger.error(f"❌ Лист 'Продукция' не найден: {e}")
            # Попробуем получить список всех листов
            all_worksheets = sheet.spreadsheet.worksheets()
            logger.info(f"Доступные листы: {[ws.title for ws in all_worksheets]}")
            return []
        # Читаем данные
        all_data = product_sheet.get_all_values()
        logger.info(f"Получено строк с листа 'Продукция': {len(all_data)}")
        
        if len(all_data) > 0:
            logger.info(f"Заголовки: {all_data[0]}")
        
        # Пропускаем заголовок (первую строку)
        products_data = all_data[1:] if len(all_data) > 1 else []
        logger.info(f"Данных товаров (без заголовка): {len(products_data)}")  
        
        # Формируем список товаров
        product_list = []
        for row in products_data:
            if len(row) >= 3 and row[0] and row[1]:  # Проверяем, что есть ID и название
                product_list.append({
                    'id': row[0].strip(),
                    'name': row[1].strip(),
                    'price': row[2].strip() if len(row) >= 3 and row[2] else '0'
                })
        
        product_last_update = time.time()
        logger.info(f"✅ Загружено {len(product_list)} товаров: {[p['name'] for p in product_list]}")
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

# Настройка логирования
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("bot.log"),  # Логи также будут сохраняться в файл
    ],
)
logger = logging.getLogger(__name__)

# Настройки из переменных окружения Railway
BOT_TOKEN = os.environ["BOT_TOKEN"]
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
DATABASE_URL = os.environ["DATABASE_URL"]
CREDENTIALS_JSON = os.environ["CREDENTIALS"]

# Список каналов продаж
SALES_CHANNELS = ["Сайт", "Инстаграм", "Телеграм", "Озон", "Маркеты"]

# Функция для подключения к БД
def get_db_connection():
    try:
        conn = psycopg2.connect(DATABASE_URL, sslmode="require")
        logger.info("✅ Успешное подключение к БД")
        return conn
    except Exception as e:
        logger.error(f"❌ Ошибка подключения к БД: {e}")
        raise

# Функция для инициализации таблицы в БД (вызывается один раз при старте)
def init_db():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        # Создаем таблицу для хранения состояний пользователей
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS user_states (
                user_id BIGINT PRIMARY KEY,
                channel VARCHAR(50),
                product_id VARCHAR(20),
                product_name VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )
        conn.commit()
        cur.close()
        conn.close()
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Error initializing database: {e}")

# Функция обновления таблиц в БД
def update_db_schema():
    """Добавляет недостающие колонки в существующую таблицу"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Проверяем существование колонок и добавляем их если нужно
        cur.execute("""
            DO $$ 
            BEGIN
                -- Добавляем product_price если не существует
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name = 'user_states' AND column_name = 'product_price'
                ) THEN
                    ALTER TABLE user_states ADD COLUMN product_price VARCHAR(20);
                END IF;
            END $$;
        """)
        
        conn.commit()
        cur.close()
        conn.close()
        logger.info("✅ Структура БД успешно обновлена")
        
    except Exception as e:
        logger.error(f"❌ Ошибка обновления структуры БД: {e}")

# Функция для авторизации и получения листа Google Sheets
def get_google_sheet():
    try:
        logger.info("=== ДИАГНОСТИКА GOOGLE SHEETS ===")
        logger.info(f"Client email: {credentials_info.get('client_email')}")
        logger.info(f"SPREADSHEET_ID: {SPREADSHEET_ID}")
        
        # Авторизация
        creds = Credentials.from_service_account_info(credentials_info, scopes=SCOPES)
        client = gspread.authorize(creds)
        
        # Пробуем получить список ВСЕХ доступных таблиц
        logger.info("Получаю список всех доступных таблиц...")
        all_spreadsheets = client.list_spreadsheet_files()
        
        if not all_spreadsheets:
            logger.error("❌ Сервисный аккаунт не видит НИКАКИХ таблиц!")
            raise PermissionError("No spreadsheets accessible")
        
        logger.info(f"Найдено таблиц: {len(all_spreadsheets)}")
        logger.info(f"Тип all_spreadsheets: {type(all_spreadsheets)}")
        logger.info(f"Первый элемент: {all_spreadsheets[0]}")
        logger.info(f"Тип первого элемента: {type(all_spreadsheets[0])}")
        
        # Ищем нашу таблицу в списке
        found = False
        for spreadsheet_data in all_spreadsheets:
            # Первый элемент - список с словарем данных таблицы
            if isinstance(spreadsheet_data, list) and len(spreadsheet_data) > 0:
                if isinstance(spreadsheet_data[0], dict) and 'id' in spreadsheet_data[0]:
                    if spreadsheet_data[0]['id'] == SPREADSHEET_ID:
                        found = True
                        logger.info(f"✅ Наша таблица найдена: {spreadsheet_data[0]['name']}")
                        break
            # Второй элемент - Response object (игнорируем)
            elif isinstance(spreadsheet_data, Response):
                logger.info("Игнорируем Response object")
                continue

        if not found:
            logger.error("❌ Наша таблица НЕ найдена в доступных")
            raise PermissionError("Spreadsheet not found in accessible list")
        
        # Если нашли - пробуем открыть
        logger.info("Пытаюсь открыть таблицу...")
        spreadsheet = client.open_by_key(SPREADSHEET_ID)
        logger.info(f"✅ Таблица открыта: {spreadsheet.title}")
        
        worksheet = spreadsheet.worksheet('Тест')
        logger.info(f"✅ Лист 'Тест' найден")
        
        return worksheet
        
    except Exception as e:
        logger.error(f"❌ Критическая ошибка в get_google_sheet: {e}", exc_info=True)
        raise

# Создаем клавиатуру с каналами продаж
def sales_channels_keyboard():

    keyboard = []
    for i in range(0, len(SALES_CHANNELS), 2):
        row = [
            InlineKeyboardButton(SALES_CHANNELS[i], callback_data=SALES_CHANNELS[i]),
            (
                InlineKeyboardButton(
                    SALES_CHANNELS[i + 1], callback_data=SALES_CHANNELS[i + 1]
                )
                if i + 1 < len(SALES_CHANNELS)
                else None
            ),
        ]
        row = [btn for btn in row if btn is not None]
        keyboard.append(row)
    return InlineKeyboardMarkup(keyboard)

# Функция генерации отчета
async def generate_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    
    try:
        # Показываем клавиатуру с вариантами отчетов
        keyboard = [
            [InlineKeyboardButton("📊 Отчет по каналам продаж", callback_data="report_channels")],
            [InlineKeyboardButton("📦 Отчет по товарам", callback_data="report_products")],
            [InlineKeyboardButton("📅 Отчет за период", callback_data="report_period")],
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
    query = update.callback_query
    user_id = query.from_user.id
    data = query.data
    
    await query.answer()
    
    if data == "report_channels":
        await generate_channels_report(query)
    elif data == "report_products":
        await generate_products_report(query)
    elif data == "report_period":
        await query.edit_message_text("📅 Введите период в формате: ДД.ММ.ГГГГ-ДД.ММ.ГГГГ\n\nПример: 01.09.2025-03.09.2025")
    elif data == "report_cancel":
        await query.edit_message_text("❌ Генерация отчета отменена")

# Функция отчета по каналам продаж
async def generate_channels_report(query):
    try:
        sheet = get_google_sheet_cached()
        
        # Получаем все данные из листа
        all_data = sheet.get_all_values()
        
        if len(all_data) <= 1:  # Только заголовок
            await query.edit_message_text("📊 Нет данных для отчета")
            return
        
        # Парсим данные (пропускаем заголовок)
        sales_data = []
        for row in all_data[1:]:
            if len(row) >= 5 and row[0] and row[2] and row[3] and row[4]:  # Проверяем обязательные поля
                try:
                    sales_data.append({
                        'channel': row[0],
                        'product': row[1],
                        'quantity': float(row[2].replace(',', '.')),
                        'price': float(row[3].replace(',', '.')),
                        'amount': float(row[4].replace(',', '.'))
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
        report_text += "Канал | Продажи | Количество | Сумма\n"
        report_text += "------------------------------------\n"
        
        total_amount = 0
        total_quantity = 0
        
        for channel, stats in channel_stats.items():
            report_text += f"{channel} | {stats['count']} | {stats['total_quantity']} | {stats['total_amount']:,.2f} руб.\n"
            total_amount += stats['total_amount']
            total_quantity += stats['total_quantity']
        
        report_text += "------------------------------------\n"
        report_text += f"ИТОГО | {len(sales_data)} | {total_quantity} | {total_amount:,.2f} руб."
        
        # Отправляем отчет
        await query.edit_message_text(report_text)
        
    except Exception as e:
        logger.error(f"❌ Ошибка генерации отчета: {e}")
        await query.edit_message_text("❌ Ошибка генерации отчета")

# Функция отчета по товарам
async def generate_products_report(query):
    try:
        sheet = get_google_sheet_cached()
        all_data = sheet.get_all_values()
        
        if len(all_data) <= 1:
            await query.edit_message_text("📦 Нет данных для отчета")
            return
        
        # Анализируем данные по товарам
        product_stats = {}
        for row in all_data[1:]:
            if len(row) >= 5 and row[1] and row[2] and row[4]:
                try:
                    product = row[1]
                    quantity = float(row[2].replace(',', '.'))
                    amount = float(row[4].replace(',', '.'))
                    
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
        report_text += "Товар | Продажи | Количество | Сумма\n"
        report_text += "------------------------------------\n"
        
        for product, stats in product_stats.items():
            report_text += f"{product} | {stats['count']} | {stats['total_quantity']} | {stats['total_amount']:,.2f} руб.\n"
        
        await query.edit_message_text(report_text)
        
    except Exception as e:
        logger.error(f"❌ Ошибка генерации отчета по товарам: {e}")
        await query.edit_message_text("❌ Ошибка генерации отчета")

# Обработчик команды /start
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_name = update.message.from_user.first_name
    help_text = f"""
Привет, {user_name}! Я бот для учета продаж.

Чтобы добавить новую запись, используй команду /add
"""
    await update.message.reply_text(help_text, parse_mode="Markdown")

# Обработчик команды /add
async def add_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id

    # Очищаем предыдущее состояние пользователя в БД
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO user_states (user_id) VALUES (%s) ON CONFLICT (user_id) DO UPDATE SET channel = NULL",
            (user_id,),
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"DB error in add_entry: {e}")
        await update.message.reply_text("❌ Ошибка сервиса. Попробуйте позже.")
        return

    # Спрашиваем канал продаж с клавиатурой
    await update.message.reply_text(
        "Выберите канал продаж:", reply_markup=sales_channels_keyboard()
    )

# Обработчик команды /report

# Обработчик нажатий на кнопки каналов продаж
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
                conn = get_db_connection()
                cur = conn.cursor()
                cur.execute(
                    "UPDATE user_states SET product_id = %s, product_name = %s, product_price = %s WHERE user_id = %s",
                    (selected_product['id'], selected_product['name'], selected_product['price'], user_id)
                )
                conn.commit()
                cur.close()
                conn.close()
                
                await query.edit_message_text(text=f"✅ Выбран товар: {selected_product['name']}")
                await query.message.reply_text("Теперь введите только количество:\n\nПример: `2`", parse_mode='Markdown')
            else:
                await query.edit_message_text(text="❌ Товар не найден")
                await query.message.reply_text("Попробуйте выбрать товар еще раз:", reply_markup=products_keyboard())
                
        except Exception as e:
            logger.error(f"DB error in product selection: {e}")
            await query.message.reply_text("❌ Ошибка сервиса. Попробуйте снова.")
    
    # 2. Обработка ОТМЕНЫ
    elif data == "cancel":
        try:
            # Очищаем состояние пользователя
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute(
                "UPDATE user_states SET channel = NULL, product_id = NULL, product_name = NULL WHERE user_id = %s",
                (user_id,)
            )
            conn.commit()
            cur.close()
            conn.close()
            
            await query.edit_message_text(text="❌ Операция отменена")
        except Exception as e:
            logger.error(f"DB error in cancel: {e}")
            await query.edit_message_text(text="❌ Операция отменена")
    
    # 3. Обработка ВЫБОРА КАНАЛА ПРОДАЖ
    elif data in SALES_CHANNELS:
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            # Очищаем предыдущий выбор товара при выборе нового канала
            cur.execute(
                "UPDATE user_states SET channel = %s, product_id = NULL, product_name = NULL WHERE user_id = %s",
                (data, user_id)
            )
            conn.commit()
            cur.close()
            conn.close()
            
            await query.edit_message_text(text=f"✅ Выбран канал: {data}")
            
            # Загружаем товары и показываем клавиатуру
            products = get_products_from_sheet()
            if products:
                await query.message.reply_text(
                    "Выберите товар:",
                    reply_markup=products_keyboard()
                )
            else:
                await query.message.reply_text("❌ Нет доступных товаров. Обратитесь к администратору.")
                
        except Exception as e:
            logger.error(f"DB error in channel selection: {e}")
            await query.answer("❌ Ошибка сохранения. Попробуйте снова.")
    
    # 4. Если callback_data не распознан
    else:
        logger.warning(f"Неизвестный callback_data: {data}")
        await query.edit_message_text(text="❌ Неизвестная команда")
        # Очищаем состояние при неизвестной команде
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute(
                "UPDATE user_states SET channel = NULL, product_id = NULL, product_name = NULL WHERE user_id = %s",
                (user_id,)
            )
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            logger.error(f"DB error cleaning state: {e}")

# Обработчик текстовых сообщений (для данных о товаре)
async def handle_product_data(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_message = update.message.text
    user_id = update.message.from_user.id
    
    # Достаем состояние пользователя из БД
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT channel, product_id, product_name, product_price FROM user_states WHERE user_id = %s", (user_id,))
        user_state = cur.fetchone()
        cur.close()
        conn.close()

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
        logger.error(f"DB error in handle_product_data: {e}")
        await update.message.reply_text("❌ Ошибка сервиса. Попробуйте позже.")
        return


    # Теперь принимаем ТОЛЬКО количество

    try:
        # Извлекаем и проверяем данные
        quantity_input = user_message.strip()
        quantity = float(quantity_input.replace(",", "."))

        if quantity <= 0:
            await update.message.reply_text("❌ Количество должно быть больше 0")
            return

        # Записываем в таблицу
        logger.info("Получаю объект листа...")
        sheet = get_google_sheet_cached()

        row_data = [channel, product_name, quantity, product_price, quantity * product_price]
        logger.info(f"Подготавливаю данные для вставки: {row_data}")

        # Пакетное обновление
        logger.info("Записываю данные пакетным обновлением...")
        try:
            # Получаем только первый столбец для поиска пустой строки
            col_a_values = sheet.col_values(1)
            
            next_row = 2
            for i, value in enumerate(col_a_values[1:], start=2):
                if not value.strip():
                    next_row = i
                    break
            else:
                next_row = len(col_a_values) + 1
            
            # Пакетное обновление - ВМЕСТО цикла с update_cell
            batch_data = []
            for col, value in enumerate(row_data, start=1):
                batch_data.append({
                    'range': f"{chr(64+col)}{next_row}",  # A2, B2, C2, etc.
                    'values': [[value]]
                })
            
            sheet.batch_update(batch_data)
            logger.info(f"✅ Пакетная запись завершена в строку {next_row}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка пакетной записи: {e}")
            raise

        # Формируем сообщение об успехе
        success_text = f"""✅ Данные успешно добавлены!
*Канал продаж:* {channel}
*Товар:* {product_name}
*Количество:* {quantity}
*Цена:* {product_price}
*Сумма:* {quantity * product_price} руб.
"""
        await update.message.reply_text(success_text, parse_mode="Markdown")
        logger.info(f"User {user_id} added record: {row_data}")

    except ValueError:
        await update.message.reply_text(
            "❌ Ошибка: количество должно быть числом. Пример: `2`"
        )
    except Exception as e:
        logger.error(f"❌ Полная ошибка при записи в Google Таблицу: {e}", exc_info=True)
        error_msg = "❌ Произошла ошибка при записи в Google Таблицу."
        await update.message.reply_text(error_msg)
        logger.error(f"Error for user {user_id}: {e}", exc_info=True)


if __name__ == "__main__":
    # Инициализируем базу данных при старте
    init_db()
    # ОБНОВЛЯЕМ СТРУКТУРУ БД ← добавляем эту строку!
    update_db_schema()

    # Парсим JSON credentials из переменной окружения
    import json

    credentials_info = json.loads(CREDENTIALS_JSON)
    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

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
    application.run_polling()
