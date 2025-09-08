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
SHEET_NAME = "Продажи - new"
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

def debug_catalog():
    """Выводит весь каталог товаров для отладки"""
    try:
        sheet = get_google_sheet_cached()
        catalog_sheet = sheet.spreadsheet.worksheet(CATALOG_SHEET_NAME)
        all_data = catalog_sheet.get_all_values()
        
        logger.info("📋 ВСЕ ЗАПИСИ В КАТАЛОГЕ ТОВАРОВ:")
        for i, row in enumerate(all_data):
            if i == 0:  # Заголовок
                logger.info(f"Заголовок: {row}")
            else:
                if len(row) >= 8:
                    logger.info(f"Строка {i+1}: {row[:8]}")  # Первые 8 колонок
                else:
                    logger.info(f"Строка {i+1}: {row} (неполная)")
                    
    except Exception as e:
        logger.error(f"❌ Ошибка при чтении каталога: {e}")

def check_catalog_structure():
    """Проверяет структуру каталога товаров"""
    try:
        sheet = get_google_sheet_cached()
        catalog_sheet = sheet.spreadsheet.worksheet(CATALOG_SHEET_NAME)
        all_data = catalog_sheet.get_all_values()
        
        logger.info("🔍 ПРОВЕРКА СТРУКТУРЫ КАТАЛОГА:")
        if len(all_data) > 0:
            logger.info(f"Заголовки: {all_data[0]}")
            
        # Проверяем первые 10 строк
        for i in range(min(11, len(all_data))):
            row = all_data[i]
            if i == 0:
                logger.info("📋 Заголовки каталога:")
            else:
                logger.info(f"📋 Строка {i}:")
            
            for col_idx, value in enumerate(row[:8]):  # Первые 8 колонок
                logger.info(f"   Колонка {col_idx}: '{value}'")
                
    except Exception as e:
        logger.error(f"❌ Ошибка проверки структуры каталога: {e}")


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
        
        logger.info(f"🔍 Поиск цены для: product_type='{product_type}', width='{width}', size='{size}', color_type='{color_type}', color='{color}'")
        
        # Исправляем значение 'None' на пустую строку
        if size == 'None':
            size = ''
        if width == 'None':
            width = ''
        
        # Функция для нормализации сравнения (приводим к нижнему регистру и убираем пробелы)
        def normalize(text):
            return str(text).lower().strip() if text else ""
        
        norm_product_type = normalize(product_type)
        norm_width = normalize(width)
        norm_size = normalize(size)
        norm_color_type = normalize(color_type)
        norm_color = normalize(color)
        
        # Пропускаем заголовок
        for i, row in enumerate(all_data[1:], start=2):
            if len(row) < 8:
                continue
                
            # Получаем значения из каталога
            catalog_product_type = normalize(row[2]) if len(row) > 2 else ""
            catalog_width = normalize(row[3]) if len(row) > 3 else ""
            catalog_size = normalize(row[4]) if len(row) > 4 else ""
            catalog_color_type = normalize(row[5]) if len(row) > 5 else ""
            catalog_color = normalize(row[6]) if len(row) > 6 else ""
            catalog_price = row[7].strip() if len(row) > 7 else ""
            
            # Логируем для отладки
            logger.info(f"📋 Сравниваем с каталогом: '{catalog_product_type}', '{catalog_width}', '{catalog_size}', '{catalog_color_type}', '{catalog_color}'")
            
            # Проверяем соответствие всех параметров
            type_match = catalog_product_type == norm_product_type
            width_match = (not norm_width) or (catalog_width == norm_width)
            size_match = (not norm_size) or (catalog_size == norm_size)
            color_type_match = catalog_color_type == norm_color_type
            color_match = catalog_color == norm_color
            
            logger.info(f"   Совпадения: Тип={type_match}, Ширина={width_match}, Размер={size_match}, ТипРасцветки={color_type_match}, Расцветка={color_match}")
            
            if (type_match and width_match and size_match and 
                color_type_match and color_match and catalog_price):
                
                try:
                    price_value = float(clean_numeric_value(catalog_price))
                    logger.info(f"✅ Найдена точная цена: {price_value} руб.")
                    return price_value
                except ValueError:
                    logger.warning(f"❌ Неверный формат цены: '{catalog_price}'")
                    continue
        
        logger.warning("🔍 Поиск по упрощенным критериям...")
        
        # Поиск только по типу товара, типу расцветки и расцветке
        for i, row in enumerate(all_data[1:], start=2):
            if len(row) < 8:
                continue
                
            catalog_product_type = normalize(row[2]) if len(row) > 2 else ""
            catalog_color_type = normalize(row[5]) if len(row) > 5 else ""
            catalog_color = normalize(row[6]) if len(row) > 6 else ""
            catalog_price = row[7].strip() if len(row) > 7 else ""
            
            if (catalog_product_type == norm_product_type and 
                catalog_color_type == norm_color_type and 
                catalog_color == norm_color and 
                catalog_price):
                
                try:
                    price_value = float(clean_numeric_value(catalog_price))
                    logger.info(f"⚠️ Найдена цена по упрощенным параметрам: {price_value} руб.")
                    return price_value
                except ValueError:
                    continue
        
        # Поиск только по типу товара и расцветке
        for i, row in enumerate(all_data[1:], start=2):
            if len(row) < 8:
                continue
                
            catalog_product_type = normalize(row[2]) if len(row) > 2 else ""
            catalog_color = normalize(row[6]) if len(row) > 6 else ""
            catalog_price = row[7].strip() if len(row) > 7 else ""
            
            if (catalog_product_type == norm_product_type and 
                catalog_color == norm_color and 
                catalog_price):
                
                try:
                    price_value = float(clean_numeric_value(catalog_price))
                    logger.info(f"⚠️ Найдена цена только по типу и расцветке: {price_value} руб.")
                    return price_value
                except ValueError:
                    continue
        
        logger.error("❌ Цена не найдена ни по одному критерию")
        
        # Выводим все записи каталога для отладки
        logger.info("📊 ВСЕ ЗАПИСИ КАТАЛОГА:")
        for i, row in enumerate(all_data):
            if i == 0:
                logger.info(f"Заголовки: {row}")
            elif len(row) >= 8:
                logger.info(f"Строка {i+1}: Тип='{row[2]}', Ширина='{row[3]}', Размер='{row[4]}', ТипРасцветки='{row[5]}', Расцветка='{row[6]}', Цена='{row[7]}'")
        
        return 0
        
    except Exception as e:
        logger.error(f"❌ Ошибка поиска цены: {e}", exc_info=True)
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


def all_colors_keyboard():
    """Клавиатура со всеми расцветками (для товаров Лежанка и Бусы)"""
    try:
        ref_data = get_reference_data()
        keyboard = []

        for color in ref_data["colors"]:
            keyboard.append(
                [InlineKeyboardButton(color, callback_data=f"color_{color}")]
            )

        keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel")])
        return InlineKeyboardMarkup(keyboard)

    except Exception as e:
        logger.error(f"❌ Ошибка создания клавиатуры всех расцветок: {e}")
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
                    # Для Лежанки и Бус - пропускаем ширину, размер и тип расцветки, переходим сразу к выбору расцветки
                    await query.message.reply_text(
                        "Выберите расцветку:", reply_markup=all_colors_keyboard()
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
                
                # Получаем текущее состояние пользователя
                cur.execute("SELECT * FROM user_states WHERE user_id = %s", (user_id,))
                user_state = cur.fetchone()
                
                # Для товаров Лежанка и Бусы устанавливаем тип расцветки как "Стандартный"
                if user_state["product_type"] in ["Лежанка", "Бусы"]:
                    cur.execute(
                        "UPDATE user_states SET color_type = 'Стандартный', color = %s WHERE user_id = %s",
                        (color, user_id),
                    )
                else:
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
                if user_state["color_type"]:
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
                user_state["color"],
            ]
        ):
            await update.message.reply_text(
                "❌ Не все параметры выбраны. Начните с /add"
            )
            return

        # Парсим количество
        try:
            quantity = int(user_message)
            if quantity <= 0:
                raise ValueError
        except ValueError:
            await update.message.reply_text(
                "❌ Пожалуйста, введите корректное количество (целое положительное число):"
            )
            return

        # Получаем цену из каталога
        price = get_product_price_from_catalog(
            user_state["product_type"],
            user_state["width"],
            user_state["size"],
            user_state["color_type"],
            user_state["color"],
        )

        total_amount = price * quantity

        # Формируем данные для записи в Google Таблицу
        row_data = [
            user_state["channel"],  # Канал продажи
            user_state["product_type"],  # Тип товара
            user_state["width"] or "",  # Ширина
            user_state["size"] or "",  # Размер
            user_state["color_type"] or "",  # Тип расцветки
            user_state["color"],  # Расцветка
            str(quantity),  # Количество
            str(price),  # Цена
            str(total_amount),  # Сумма
            datetime.now().strftime("%d.%m.%Y"),  # Дата
        ]

        # Записываем в Google Таблицу
        try:
            sheet = get_google_sheet_cached()
            sheet.append_row(row_data)
            logger.info(f"✅ Запись добавлена в Google Таблицу: {row_data}")
        except Exception as e:
            logger.error(f"❌ Ошибка записи в Google Таблицу: {e}")
            await update.message.reply_text("❌ Ошибка записи данных")
            return

        # Формируем название товара для сообщения
        product_name_parts = [user_state["product_type"]]
        if user_state["width"]:
            product_name_parts.append(user_state["width"])
        if user_state["size"]:
            product_name_parts.append(user_state["size"])
        if user_state["color_type"]:
            product_name_parts.append(user_state["color_type"])
        product_name_parts.append(user_state["color"])
        product_name = " ".join(product_name_parts)

        # Отправляем подтверждение
        await update.message.reply_text(
            f"✅ *Продажа добавлена!*\n\n"
            f"*Канал:* {user_state['channel']}\n"
            f"*Товар:* {product_name}\n"
            f"*Количество:* {quantity} шт.\n"
            f"*Цена:* {price:.2f} руб.\n"
            f"*Сумма:* {total_amount:.2f} руб.\n\n"
            f"Для новой записи используйте /add",
            parse_mode="Markdown",
        )

        # Очищаем состояние пользователя
        with get_db_cursor() as cur:
            cur.execute(
                "UPDATE user_states SET channel = NULL, product_type = NULL, width = NULL, size = NULL, color_type = NULL, color = NULL WHERE user_id = %s",
                (user_id,),
            )

    except Exception as e:
        logger.error(f"❌ Ошибка в handle_quantity_input: {e}")
        await update.message.reply_text("❌ Ошибка обработки запроса")


# ==================== ОСНОВНАЯ ФУНКЦИЯ ====================
def main():
    """Основная функция запуска бота"""
    try:
        # Инициализация БД
        init_db()

        # Создание приложения
        application = Application.builder().token(BOT_TOKEN).build()

        # Добавление обработчиков
        application.add_handler(CommandHandler("start", start))
        application.add_handler(CommandHandler("add", add_entry))
        application.add_handler(CallbackQueryHandler(button_handler))
        application.add_handler(
            MessageHandler(filters.TEXT & ~filters.COMMAND, handle_quantity_input)
        )

        # Запуск бота
        logger.info("🤖 Бот запущен...")
        application.run_polling()

    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        raise


if __name__ == "__main__":
    main()