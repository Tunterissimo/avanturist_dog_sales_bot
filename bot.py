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


# Обработчик нажатий на кнопки каналов продаж
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    selected_channel = query.data

    # Сохраняем выбранный канал продаж в БД
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "UPDATE user_states SET channel = %s WHERE user_id = %s",
            (selected_channel, user_id),
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"DB error in button_handler: {e}")
        await query.answer("❌ Ошибка сохранения. Попробуйте снова.")
        return

    # Подтверждаем нажатие кнопки
    await query.answer()
    await query.edit_message_text(text=f"✅ Выбран канал: {selected_channel}")

    # Просим ввести остальные данные
    instruction_text = """
Теперь введите остальные данные через запятую в формате:
*Наименование товара, Количество, Цена*

Например:
`Ошейник для собаки, 2, 1500`
"""
    await query.message.reply_text(instruction_text, parse_mode="Markdown")


# Обработчик текстовых сообщений (для данных о товаре)
async def handle_product_data(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_message = update.message.text
    user_id = update.message.from_user.id

    # Достаем состояние пользователя из БД
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT channel FROM user_states WHERE user_id = %s", (user_id,))
        user_state = cur.fetchone()
        cur.close()
        conn.close()

        if not user_state or not user_state["channel"]:
            await update.message.reply_text(
                "❌ Сначала выберите канал продаж с помощью команды /add"
            )
            return

        channel = user_state["channel"]

    except Exception as e:
        logger.error(f"DB error in handle_product_data: {e}")
        await update.message.reply_text("❌ Ошибка сервиса. Попробуйте позже.")
        return

    # Разбиваем сообщение по запятым
    data = [item.strip() for item in user_message.split(",")]

    # Проверяем, что получили ровно 3 элемента
    if len(data) != 3:
        error_text = """Неверный формат данных. Нужно указать 3 значения через запятую:
*Наименование товара, Количество, Цена*

Пример:
`Ошейник для собаки, 2, 1500`"""
        await update.message.reply_text(error_text, parse_mode="Markdown")
        return

    try:
        # Извлекаем и проверяем данные
        product, quantity, price = data
        quantity = float(quantity.replace(",", "."))
        price = float(price.replace(",", "."))

        # Записываем в таблицу
        logger.info("Получаю объект листа...")
        sheet = get_google_sheet()

        row_data = [channel, product, quantity, price]
        logger.info(f"Подготавливаю данные для вставки: {row_data}")

        logger.info("Определяю следующую строку для вставки...")
        next_row = len(sheet.get_all_values()) + 1

        logger.info("Вставляю строку...")
        sheet.insert_row(row_data, next_row)
        logger.info("✅ Данные успешно вставлены")

        # Формируем сообщение об успехе
        success_text = f"""✅ Данные успешно добавлены!
*Канал продаж:* {channel}
*Товар:* {product}
*Количество:* {quantity}
*Цена:* {price}
*Сумма:* {quantity * price} руб.
"""
        await update.message.reply_text(success_text, parse_mode="Markdown")
        logger.info(f"User {user_id} added record: {row_data}")

    except ValueError:
        await update.message.reply_text(
            "Ошибка: 'Количество' и 'Цена' должны быть числами."
        )
    except Exception as e:
        logger.error(f"❌ Полная ошибка при записи в Google Таблицу: {e}", exc_info=True)
        error_msg = "❌ Произошла ошибка при записи в Google Таблицу."
        await update.message.reply_text(error_msg)
        logger.error(f"Error for user {user_id}: {e}", exc_info=True)


if __name__ == "__main__":
    # Инициализируем базу данных при старте
    init_db()

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
    application.add_handler(CallbackQueryHandler(button_handler))
    application.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, handle_product_data)
    )

    # Запускаем бота
    application.run_polling()
