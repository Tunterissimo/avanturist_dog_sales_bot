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
SHEET_NAME = "Продажи"
NEW_SHEET_NAME = "Продажи - new"
PRODUCT_SHEET_NAME = "Продукция"
CATALOG_SHEET_NAME = "Каталог товаров"
CHANNELS_SHEET_NAME = "Каналы"
REFERENCE_SHEET_NAME = "Справочники"

# Константы для справочников
PRODUCT_TYPES_HEADER = "ТИПЫ ТОВАРОВ"
WIDTHS_HEADER = "ШИРИНЫ СТРОП"
COLOR_TYPES_HEADER = "ТИПЫ РАСЦВЕТОК"
COLORS_HEADER = "РАСЦВЕТКИ"


# ==================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ====================
def clean_numeric_value(value):
    """Очищает числовое значение от символов валюты и пробелов"""
    if not value:
        return "0"

    # Если значение уже чистое число (новый формат)
    if isinstance(value, (int, float)):
        return str(value)

    # Если значение строка со старым форматом
    cleaned = (
        value.replace("р.", "").replace(" ", "").replace("\xa0", "").replace(",", ".")
    )
    return cleaned.strip()


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
    """Инициализация таблицы в БД с новыми полями"""
    try:
        with get_db_cursor() as cur:
            # Удаляем старую таблицу если она существует
            cur.execute("DROP TABLE IF EXISTS user_states")

            # Создаем новую таблицу с правильной структурой
            cur.execute(
                """
                CREATE TABLE user_states (
                    user_id BIGINT PRIMARY KEY,
                    channel VARCHAR(50),
                    product_type VARCHAR(50),
                    width VARCHAR(20),
                    size VARCHAR(20),
                    color_type VARCHAR(50),
                    color VARCHAR(50),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            )
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
def get_channels_from_sheet():
    """Загружает список каналов продаж из Google Таблицы с кешированием"""
    try:
        logger.info("🔄 Загружаю список каналов из Google Таблицы...")
        sheet = get_google_sheet_cached()

        try:
            channels_sheet = sheet.spreadsheet.worksheet(CHANNELS_SHEET_NAME)
            logger.info("✅ Лист 'Каналы' найден")
        except Exception as e:
            logger.error(f"❌ Лист 'Каналы' не найден: {e}")
            return []

        all_data = channels_sheet.get_all_values()
        logger.info(f"📊 Получено строк с листа 'Каналы': {len(all_data)}")

        # Пропускаем заголовок
        channels_data = all_data[1:] if len(all_data) > 1 else []

        # Формируем список каналов
        channels_list = []
        for row in channels_data:
            if len(row) >= 2 and row[0] and row[1]:
                channels_list.append(row[1].strip())

        logger.info(f"✅ Загружено {len(channels_list)} каналов: {channels_list}")
        return channels_list

    except Exception as e:
        logger.error(f"❌ Ошибка загрузки каналов: {e}")
        return []


@lru_cache(maxsize=1)
def get_reference_data():
    """Загружает данные из справочников"""
    try:
        logger.info("🔄 Загружаю данные из справочников...")
        sheet = get_google_sheet_cached()

        try:
            ref_sheet = sheet.spreadsheet.worksheet(REFERENCE_SHEET_NAME)
            all_data = ref_sheet.get_all_values()
        except Exception as e:
            logger.error(f"❌ Лист '{REFERENCE_SHEET_NAME}' не найден: {e}")
            return {}

        reference_data = {
            "product_types": [],
            "widths": [],
            "color_types": [],
            "colors": [],
        }

        current_section = None

        for row in all_data:
            if not any(row):
                continue

            # Определяем текущий раздел
            if PRODUCT_TYPES_HEADER in row[0]:
                current_section = "product_types"
                continue
            elif WIDTHS_HEADER in row[0]:
                current_section = "widths"
                continue
            elif COLOR_TYPES_HEADER in row[0]:
                current_section = "color_types"
                continue
            elif COLORS_HEADER in row[0]:
                current_section = "colors"
                continue

            # Парсим данные в зависимости от раздела
            if current_section == "product_types" and len(row) >= 3:
                if row[0] and row[0] != "Тип товара":  # Пропускаем заголовок
                    reference_data["product_types"].append(
                        {
                            "type": row[0].strip(),
                            "has_width": row[1].strip().lower() == "да",
                            "has_size": row[2].strip().lower() == "да",
                        }
                    )

            elif current_section == "widths" and len(row) >= 2:
                if row[0] and row[0] != "Ширина":  # Пропускаем заголовок
                    available_sizes = (
                        [s.strip() for s in row[1].split(",")] if row[1] else []
                    )
                    reference_data["widths"].append(
                        {"width": row[0].strip(), "available_sizes": available_sizes}
                    )

            elif current_section == "color_types" and len(row) >= 2:
                if row[0] and row[0] != "Тип расцветки":  # Пропускаем заголовок
                    available_colors = (
                        [c.strip() for c in row[1].split(",")] if row[1] else []
                    )
                    reference_data["color_types"].append(
                        {"type": row[0].strip(), "available_colors": available_colors}
                    )

            elif current_section == "colors" and row[0]:
                if row[0] != "Расцветка":  # Пропускаем заголовок
                    reference_data["colors"].append(row[0].strip())

        logger.info(
            f"✅ Загружены справочники: {len(reference_data['product_types'])} типов товаров, "
            f"{len(reference_data['widths'])} ширин, {len(reference_data['color_types'])} типов расцветок, "
            f"{len(reference_data['colors'])} расцветок"
        )

        return reference_data

    except Exception as e:
        logger.error(f"❌ Ошибка загрузки справочников: {e}")
        return {}


def get_product_price_from_catalog(product_type, width, size, color_type, color):
    """Находит цену товара в каталоге по параметрам"""
    try:
        sheet = get_google_sheet_cached()
        catalog_sheet = sheet.spreadsheet.worksheet(CATALOG_SHEET_NAME)
        all_data = catalog_sheet.get_all_values()
        
        logger.info(f"🔍 Поиск цены для: {product_type}, {width}, {size}, {color_type}, {color}")
        logger.info(f"📊 Всего строк в каталоге: {len(all_data)}")
        
        # Пропускаем заголовок
        for i, row in enumerate(all_data[1:], start=2):
            if len(row) < 8:
                continue
                
            # Логируем каждую строку для отладки
            if i <= 10:  # Логируем первые 10 строк
                logger.info(f"Строка {i}: {row}")
            
            # Проверяем соответствие всем параметрам
            catalog_product_type = row[2].strip() if len(row) > 2 else ""
            catalog_width = row[3].strip() if len(row) > 3 else ""
            catalog_size = row[4].strip() if len(row) > 4 else ""
            catalog_color_type = row[5].strip() if len(row) > 5 else ""
            catalog_color = row[6].strip() if len(row) > 6 else ""
            catalog_price = row[7].strip() if len(row) > 7 else ""
            
            # Проверяем соответствие (учитываем, что некоторые параметры могут быть пустыми)
            type_match = catalog_product_type == product_type
            width_match = (not width) or (catalog_width == width) or (width == "" and catalog_width == "")
            size_match = (not size) or (catalog_size == size) or (size == "" and catalog_size == "")
            color_type_match = catalog_color_type == color_type
            color_match = catalog_color == color
            
            if (type_match and width_match and size_match and 
                color_type_match and color_match and catalog_price):
                
                price_value = float(clean_numeric_value(catalog_price))
                logger.info(f"✅ Найдена цена: {price_value} руб. для строки {i}")
                return price_value
        
        logger.warning(f"❌ Цена не найдена для: {product_type}, {width}, {size}, {color_type}, {color}")
        # Попробуем найти хотя бы по основным параметрам
        for i, row in enumerate(all_data[1:], start=2):
            if len(row) < 8:
                continue
                
            catalog_product_type = row[2].strip() if len(row) > 2 else ""
            catalog_color_type = row[5].strip() if len(row) > 5 else ""
            catalog_color = row[6].strip() if len(row) > 6 else ""
            catalog_price = row[7].strip() if len(row) > 7 else ""
            
            if (catalog_product_type == product_type and 
                catalog_color_type == color_type and 
                catalog_color == color and 
                catalog_price):
                
                price_value = float(clean_numeric_value(catalog_price))
                logger.info(f"⚠️ Найдена цена по упрощенным параметрам: {price_value} руб.")
                return price_value
        
        logger.error(f"❌ Цена не найдена даже по упрощенным параметрам")
        return 0
        
    except Exception as e:
        logger.error(f"❌ Ошибка поиска цены: {e}")
        return 0


# ==================== КЛАВИАТУРЫ ====================
def sales_channels_keyboard():
    """Создает клавиатуру с каналами продаж из Google Таблицы"""
    try:
        channels = get_channels_from_sheet()
        keyboard = []

        # Создаем кнопки (по 2 в ряд)
        for i in range(0, len(channels), 2):
            row = []
            row.append(InlineKeyboardButton(channels[i], callback_data=channels[i]))

            if i + 1 < len(channels):
                row.append(
                    InlineKeyboardButton(channels[i + 1], callback_data=channels[i + 1])
                )

            keyboard.append(row)

        # Добавляем кнопку "Отмена"
        keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel")])

        return InlineKeyboardMarkup(keyboard)
    except Exception as e:
        logger.error(f"❌ Ошибка создания клавиатуры каналов: {e}")
        return InlineKeyboardMarkup(
            [[InlineKeyboardButton("❌ Отмена", callback_data="cancel")]]
        )


def product_types_keyboard():
    """Клавиатура с типами товаров"""
    try:
        ref_data = get_reference_data()
        keyboard = []

        for product_type in ref_data["product_types"]:
            keyboard.append(
                [
                    InlineKeyboardButton(
                        product_type["type"],
                        callback_data=f"type_{product_type['type']}",
                    )
                ]
            )

        keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel")])
        return InlineKeyboardMarkup(keyboard)

    except Exception as e:
        logger.error(f"❌ Ошибка создания клавиатуры типов товаров: {e}")
        return InlineKeyboardMarkup(
            [[InlineKeyboardButton("❌ Отмена", callback_data="cancel")]]
        )


def widths_keyboard():
    """Клавиатура с ширинами строп"""
    try:
        ref_data = get_reference_data()
        keyboard = []

        for width_data in ref_data["widths"]:
            keyboard.append(
                [
                    InlineKeyboardButton(
                        width_data["width"],
                        callback_data=f"width_{width_data['width']}",
                    )
                ]
            )

        keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel")])
        return InlineKeyboardMarkup(keyboard)

    except Exception as e:
        logger.error(f"❌ Ошибка создания клавиатуры ширин: {e}")
        return InlineKeyboardMarkup(
            [[InlineKeyboardButton("❌ Отмена", callback_data="cancel")]]
        )


def sizes_keyboard(selected_width):
    """Клавиатура с размерами для выбранной ширины"""
    try:
        ref_data = get_reference_data()
        keyboard = []

        # Находим доступные размеры для выбранной ширины
        width_data = next(
            (w for w in ref_data["widths"] if w["width"] == selected_width), None
        )

        if width_data:
            for size in width_data["available_sizes"]:
                keyboard.append(
                    [InlineKeyboardButton(size, callback_data=f"size_{size}")]
                )

        keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel")])
        return InlineKeyboardMarkup(keyboard)

    except Exception as e:
        logger.error(f"❌ Ошибка создания клавиатуры размеров: {e}")
        return InlineKeyboardMarkup(
            [[InlineKeyboardButton("❌ Отмена", callback_data="cancel")]]
        )


def color_types_keyboard():
    """Клавиатура с типами расцветок"""
    try:
        ref_data = get_reference_data()
        keyboard = []

        for color_type in ref_data["color_types"]:
            keyboard.append(
                [
                    InlineKeyboardButton(
                        color_type["type"],
                        callback_data=f"colortype_{color_type['type']}",
                    )
                ]
            )

        keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel")])
        return InlineKeyboardMarkup(keyboard)

    except Exception as e:
        logger.error(f"❌ Ошибка создания клавиатуры типов расцветок: {e}")
        return InlineKeyboardMarkup(
            [[InlineKeyboardButton("❌ Отмена", callback_data="cancel")]]
        )


def colors_keyboard(selected_color_type):
    """Клавиатура с расцветками для выбранного типа"""
    try:
        ref_data = get_reference_data()
        keyboard = []

        # Находим доступные расцветки для выбранного типа
        color_type_data = next(
            (ct for ct in ref_data["color_types"] if ct["type"] == selected_color_type),
            None,
        )

        if color_type_data:
            for color in color_type_data["available_colors"]:
                keyboard.append(
                    [InlineKeyboardButton(color, callback_data=f"color_{color}")]
                )

        keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel")])
        return InlineKeyboardMarkup(keyboard)

    except Exception as e:
        logger.error(f"❌ Ошибка создания клавиатуры расцветок: {e}")
        return InlineKeyboardMarkup(
            [[InlineKeyboardButton("❌ Отмена", callback_data="cancel")]]
        )


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
    """Обработчик команды /add для нового процесса"""
    user_id = update.message.from_user.id

    # Очищаем предыдущее состояние пользователя в БД
    try:
        with get_db_cursor() as cur:
            cur.execute(
                "INSERT INTO user_states (user_id, channel, product_type, width, size, color_type, color) VALUES (%s, NULL, NULL, NULL, NULL, NULL, NULL) ON CONFLICT (user_id) DO UPDATE SET channel = NULL, product_type = NULL, width = NULL, size = NULL, color_type = NULL, color = NULL",
                (user_id,),
            )
    except Exception as e:
        logger.error(f"❌ Ошибка БД в add_entry: {e}")
        await update.message.reply_text("❌ Ошибка сервиса. Попробуйте позже.")
        return

    # Спрашиваем канал продаж с клавиатуряой
    await update.message.reply_text(
        "Выберите канал продаж:", reply_markup=sales_channels_keyboard()
    )


# ==================== ОБРАБОТЧИКИ КНОПОК ====================
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик нажатий на инлайн-кнопки для нового процесса"""
    query = update.callback_query
    user_id = query.from_user.id
    data = query.data

    await query.answer()

    try:
        with get_db_cursor() as cur:
            cur.execute("SELECT * FROM user_states WHERE user_id = %s", (user_id,))
            user_state = cur.fetchone()

            if not user_state:
                await query.edit_message_text("❌ Сессия истекла. Начните с /add")
                return

            # 1. Обработка ВЫБОРА КАНАЛА ПРОДАЖ
            if data in get_channels_from_sheet():
                cur.execute(
                    "UPDATE user_states SET channel = %s, product_type = NULL, width = NULL, size = NULL, color_type = NULL, color = NULL WHERE user_id = %s",
                    (data, user_id),
                )
                await query.edit_message_text(text=f"✅ Выбран канал: {data}")
                await query.message.reply_text(
                    "Выберите тип товара:", reply_markup=product_types_keyboard()
                )

            # 2. Обработка ВЫБОРА ТИПА ТОВАРА
            elif data.startswith("type_"):
                product_type = data.split("_", 1)[1]
                ref_data = get_reference_data()
                product_type_data = next(
                    (
                        pt
                        for pt in ref_data["product_types"]
                        if pt["type"] == product_type
                    ),
                    None,
                )

                if not product_type_data:
                    await query.edit_message_text("❌ Тип товара не найден")
                    return

                cur.execute(
                    "UPDATE user_states SET product_type = %s, width = NULL, size = NULL WHERE user_id = %s",
                    (product_type, user_id),
                )

                await query.edit_message_text(text=f"✅ Выбран тип: {product_type}")

                # Проверяем правила бизнес-процесса
                if product_type in ["Лежанка", "Бусы"]:
                    # Пропускаем ширину и размер, переходим к типу расцветки
                    await query.message.reply_text(
                        "Выберите тип расцветки:", reply_markup=color_types_keyboard()
                    )
                elif product_type_data["has_width"]:
                    await query.message.reply_text(
                        "Выберите ширину стропы:", reply_markup=widths_keyboard()
                    )
                else:
                    await query.message.reply_text(
                        "Выберите тип расцветки:", reply_markup=color_types_keyboard()
                    )

            # 3. Обработка ВЫБОРА ШИРИНЫ
            elif data.startswith("width_"):
                width = data.split("_", 1)[1]
                cur.execute(
                    "UPDATE user_states SET width = %s, size = NULL WHERE user_id = %s",
                    (width, user_id),
                )

                await query.edit_message_text(text=f"✅ Выбрана ширина: {width}")

                # Проверяем нужен ли размер
                cur.execute(
                    "SELECT product_type FROM user_states WHERE user_id = %s",
                    (user_id,),
                )
                product_type = cur.fetchone()["product_type"]
                ref_data = get_reference_data()
                product_type_data = next(
                    (
                        pt
                        for pt in ref_data["product_types"]
                        if pt["type"] == product_type
                    ),
                    None,
                )

                if product_type_data and product_type_data["has_size"]:
                    await query.message.reply_text(
                        "Выберите размер:", reply_markup=sizes_keyboard(width)
                    )
                else:
                    await query.message.reply_text(
                        "Выберите тип расцветки:", reply_markup=color_types_keyboard()
                    )

            # 4. Обработка ВЫБОРА РАЗМЕРА
            elif data.startswith("size_"):
                size = data.split("_", 1)[1]
                cur.execute(
                    "UPDATE user_states SET size = %s WHERE user_id = %s",
                    (size, user_id),
                )

                await query.edit_message_text(text=f"✅ Выбран размер: {size}")
                await query.message.reply_text(
                    "Выберите тип расцветки:", reply_markup=color_types_keyboard()
                )

            # 5. Обработка ВЫБОРА ТИПА РАСЦВЕТКИ
            elif data.startswith("colortype_"):
                color_type = data.split("_", 1)[1]
                cur.execute(
                    "UPDATE user_states SET color_type = %s, color = NULL WHERE user_id = %s",
                    (color_type, user_id),
                )

                await query.edit_message_text(
                    text=f"✅ Выбран тип расцветки: {color_type}"
                )
                await query.message.reply_text(
                    "Выберите расцветку:", reply_markup=colors_keyboard(color_type)
                )

            # 6. Обработка ВЫБОРА РАСЦВЕТКИ
            elif data.startswith("color_"):
                color = data.split("_", 1)[1]
                cur.execute(
                    "UPDATE user_states SET color = %s WHERE user_id = %s",
                    (color, user_id),
                )

                # Получаем все выбранные параметры для формирования названия товара
                cur.execute("SELECT * FROM user_states WHERE user_id = %s", (user_id,))
                user_state = cur.fetchone()

                # Логируем параметры для отладки
                logger.info(f"🎯 Параметры товара: "
                        f"Тип={user_state['product_type']}, "
                        f"Ширина={user_state['width']}, "
                        f"Размер={user_state['size']}, "
                        f"ТипРасцветки={user_state['color_type']}, "
                        f"Расцветка={user_state['color']}")

                # Формируем название товара
                product_name_parts = [user_state["product_type"]]
                if user_state["width"]:
                    product_name_parts.append(user_state["width"])
                if user_state["size"]:
                    product_name_parts.append(user_state["size"])
                product_name_parts.append(user_state["color_type"])
                product_name_parts.append(user_state["color"])

                product_name = " ".join(product_name_parts)

                # Находим цену
                price = get_product_price_from_catalog(
                    user_state["product_type"],
                    user_state["width"],
                    user_state["size"],
                    user_state["color_type"],
                    user_state["color"],
                )

                await query.edit_message_text(text=f"✅ Выбрана расцветка: {color}")
                await query.message.reply_text(
                    f"🎯 Все параметры выбраны!\n\n"
                    f"*Товар:* {product_name}\n"
                    f"*Цена:* {price:.2f} руб.\n\n"
                    f"Теперь введите количество:",
                    parse_mode="Markdown",
                )

            # 7. Обработка ОТМЕНЫ
            elif data == "cancel":
                cur.execute(
                    "UPDATE user_states SET channel = NULL, product_type = NULL, width = NULL, size = NULL, color_type = NULL, color = NULL WHERE user_id = %s",
                    (user_id,),
                )
                await query.edit_message_text(text="❌ Операция отменена")

            else:
                logger.warning(f"⚠️ Неизвестный callback_data: {data}")
                await query.edit_message_text(text="❌ Неизвестная команда")

    except Exception as e:
        logger.error(f"❌ Ошибка в button_handler: {e}")
        await query.edit_message_text("❌ Ошибка обработки запроса")


# ==================== ОБРАБОТЧИК ТЕКСТОВЫХ СООБЩЕНИЙ ====================
async def handle_quantity_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик ввода количества для нового процесса"""
    user_message = update.message.text
    user_id = update.message.from_user.id

    try:
        with get_db_cursor() as cur:
            cur.execute("SELECT * FROM user_states WHERE user_id = %s", (user_id,))
            user_state = cur.fetchone()

        if not user_state or not all(
            [
                user_state["channel"],
                user_state["product_type"],
                user_state["color_type"],
                user_state["color"],
            ]
        ):
            await update.message.reply_text(
                "❌ Не все параметры выбраны. Начните с /add"
            )
            return

        # Принимаем количество
        try:
            quantity = float(user_message.strip().replace(",", "."))
            if quantity <= 0:
                await update.message.reply_text("❌ Количество должно быть больше 0")
                return

            # Находим цену
            price = get_product_price_from_catalog(
                user_state["product_type"],
                user_state["width"],
                user_state["size"],
                user_state["color_type"],
                user_state["color"],
            )

            total_amount = quantity * price

            # Формируем название товара
            product_name_parts = [user_state["product_type"]]
            if user_state["width"]:
                product_name_parts.append(user_state["width"])
            if user_state["size"]:
                product_name_parts.append(user_state["size"])
            product_name_parts.append(user_state["color_type"])
            product_name_parts.append(user_state["color"])
            product_name = " ".join(product_name_parts)

            # Записываем в новую таблицу
            sheet = get_google_sheet_cached()
            new_sheet = sheet.spreadsheet.worksheet(NEW_SHEET_NAME)

            row_data = [
                user_state["channel"],
                user_state["product_type"],
                user_state["width"] or "",
                user_state["size"] or "",
                user_state["color_type"],
                user_state["color"],
                quantity,
                price,
                total_amount,
                datetime.now().strftime("%d.%m.%Y"),
            ]

            new_sheet.append_row(row_data, value_input_option="USER_ENTERED")

            # Формируем сообщение об успехе
            success_text = f"""✅ Данные успешно добавлены в новую таблицу!

*Канал продаж:* {user_state["channel"]}
*Товар:* {product_name}
*Количество:* {quantity}
*Цена:* {price:.2f} руб.
*Сумма:* {total_amount:.2f} руб.
"""
            await update.message.reply_text(success_text, parse_mode="Markdown")
            logger.info(f"👤 User {user_id} added new record: {row_data}")

            # Очищаем состояние пользователя
            with get_db_cursor() as cur:
                cur.execute(
                    "UPDATE user_states SET channel = NULL, product_type = NULL, width = NULL, size = NULL, color_type = NULL, color = NULL WHERE user_id = %s",
                    (user_id,),
                )

        except ValueError:
            await update.message.reply_text(
                "❌ Ошибка: количество должно быть числом. Пример: `2` или `1.5`",
                parse_mode="Markdown",
            )

    except Exception as e:
        logger.error(f"❌ Ошибка при записи в Google Таблицу: {e}", exc_info=True)
        await update.message.reply_text(
            "❌ Произошла ошибка при записи. Попробуйте позже."
        )


# ==================== ОТЧЕТЫ ====================
async def generate_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Генерация отчета"""
    try:
        keyboard = [
            [
                InlineKeyboardButton(
                    "📊 Отчет по каналам продаж", callback_data="report_channels"
                )
            ],
            [
                InlineKeyboardButton(
                    "📦 Отчет по товарам", callback_data="report_products"
                )
            ],
            [InlineKeyboardButton("❌ Отмена", callback_data="report_cancel")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await update.message.reply_text(
            "📈 Выберите тип отчета:", reply_markup=reply_markup
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

        logger.info(f"Всего строк данных: {len(all_data)}")
        if len(all_data) > 1:
            logger.info(f"Первые 3 строки данных: {all_data[1:4]}")

        if len(all_data) <= 1:
            await query.edit_message_text("📊 Нет данных для отчета")
            return

        # Находим индексы колонок по заголовкам
        headers = all_data[0]
        logger.info(f"Заголовки таблицы: {headers}")
        try:
            channel_idx = headers.index("Канал продаж")
            product_idx = headers.index("Наименование товара")
            qty_idx = headers.index("Количество")
            price_idx = headers.index("Цена")
            amount_idx = headers.index("Сумма")
        except ValueError as e:
            logger.error(f"❌ Не найдена ожидаемая колонка: {e}. Заголовки: {headers}")
            await query.edit_message_text(
                "❌ Ошибка: таблица имеет неверную структуру."
            )
            return

        # Парсим данные
        sales_data = []
        for row in all_data[1:]:
            # Пропускаем пустые строки
            if not any(row) or len(row) < 6:  # Проверяем, что есть все 6 колонок
                continue

            try:
                # Очищаем числовые значения
                cleaned_qty = clean_numeric_value(row[qty_idx])
                cleaned_amount = clean_numeric_value(row[amount_idx])
                cleaned_price = (
                    clean_numeric_value(row[price_idx])
                    if len(row) > price_idx and row[price_idx]
                    else "0"
                )

                sales_data.append(
                    {
                        "channel": row[channel_idx],
                        "product": row[product_idx] if len(row) > product_idx else "",
                        "quantity": float(cleaned_qty),
                        "price": float(cleaned_price),
                        "amount": float(cleaned_amount),
                    }
                )
            except (ValueError, IndexError) as e:
                logger.warning(
                    f"Пропущена строка из-за ошибки формата: {row}. Ошибка: {e}"
                )
                continue

        if not sales_data:
            await query.edit_message_text("📊 Нет данных для анализа")
            return

        # Анализируем данные по каналам
        channel_stats = {}
        for sale in sales_data:
            channel = sale["channel"]
            # Проверяем, что канал есть в актуальном списке
            available_channels = get_channels_from_sheet()
            if channel not in available_channels:
                logger.warning(f"⚠️ Неизвестный канал в данных: {channel}")
                continue

            if channel not in channel_stats:
                channel_stats[channel] = {
                    "count": 0,
                    "total_amount": 0,
                    "total_quantity": 0,
                }

            channel_stats[channel]["count"] += 1
            channel_stats[channel]["total_amount"] += sale["amount"]
            channel_stats[channel]["total_quantity"] += sale["quantity"]

        # Формируем отчет
        report_text = "📊 ОТЧЕТ ПО КАНАЛАм ПРОДАЖ\n\n"
        report_text += "```\n"
        report_text += "Канал         | Продажи | Количество | Сумма       \n"
        report_text += "--------------+---------+------------+-------------\n"

        total_amount = 0
        total_quantity = 0
        total_sales = len(sales_data)

        for channel, stats in sorted(channel_stats.items()):
            report_text += f"{channel:<13} | {stats['count']:>7} | {stats['total_quantity']:>10.2f} | {stats['total_amount']:>11.2f}\n"
            total_amount += stats["total_amount"]
            total_quantity += stats["total_quantity"]

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

        logger.info(f"Всего строк данных: {len(all_data)}")
        if len(all_data) > 1:
            logger.info(f"Первые 3 строки данных: {all_data[1:4]}")

        if len(all_data) <= 1:
            await query.edit_message_text("📦 Нет данных для отчета")
            return

        # Находим индексы колонок по заголовкам
        headers = all_data[0]
        logger.info(f"Заголовки таблица: {headers}")
        try:
            product_idx = headers.index("Наименование товара")
            qty_idx = headers.index("Количество")
            amount_idx = headers.index("Сумма")
        except ValueError as e:
            logger.error(f"❌ Не найдена ожидаемая колонка: {e}. Заголовки: {headers}")
            await query.edit_message_text(
                "❌ Ошибка: таблица имеет неверную структуру."
            )
            return

        # Анализируем данные по товарам
        product_stats = {}
        for row in all_data[1:]:
            # Пропускаем пустые строки
            if not any(row) or len(row) < 6:
                continue

            try:
                # Очищаем числовые значения
                cleaned_qty = clean_numeric_value(row[qty_idx])
                cleaned_amount = clean_numeric_value(row[amount_idx])

                product = row[product_idx]
                quantity = float(cleaned_qty)
                amount = float(cleaned_amount)

                if product not in product_stats:
                    product_stats[product] = {
                        "count": 0,
                        "total_amount": 0,
                        "total_quantity": 0,
                    }

                product_stats[product]["count"] += 1
                product_stats[product]["total_amount"] += amount
                product_stats[product]["total_quantity"] += quantity

            except (ValueError, IndexError) as e:
                logger.warning(
                    f"Пропущена строка из-за ошибки формата: {row}. Ошибка: {e}"
                )
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
        MessageHandler(filters.TEXT & ~filters.COMMAND, handle_quantity_input)
    )

    # Запускаем бота
    logger.info("🤖 Бот запущен с новым процессом...")
    application.run_polling()
