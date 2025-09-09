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
from datetime import datetime, timedelta

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
CATALOG_SHEET_NAME = "Каталог товаров"
CHANNELS_SHEET_NAME = "Каналы"
REFERENCE_SHEET_NAME = "Справочники"
PAYMENT_METHODS_SHEET_NAME = "Способы оплаты"

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
                    payment_method VARCHAR(50),
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
def get_payment_methods_from_sheet():
    """Загружает список способов оплаты из Google Таблицы с кешированием"""
    try:
        logger.info("🔄 Загружаю список способов оплаты из Google Таблицы...")
        sheet = get_google_sheet_cached()

        try:
            payment_sheet = sheet.spreadsheet.worksheet(PAYMENT_METHODS_SHEET_NAME)
            logger.info("✅ Лист 'Способы оплаты' найден")
        except Exception as e:
            logger.error(f"❌ Лист 'Способы оплаты' не найден: {e}")
            return ["ИП", "Перевод", "Наличные"]  # Fallback значения

        all_data = payment_sheet.get_all_values()
        logger.info(f"📊 Получено строк с листа 'Способы оплаты': {len(all_data)}")

        # Пропускаем заголовок
        payment_data = all_data[1:] if len(all_data) > 1 else []

        # Формируем список способов оплаты
        payment_list = []
        for row in payment_data:
            if len(row) >= 2 and row[1]:  # Берем значение из колонки "Наименование"
                payment_list.append(row[1].strip())

        logger.info(f"✅ Загружено {len(payment_list)} способов оплаты: {payment_list}")
        return payment_list

    except Exception as e:
        logger.error(f"❌ Ошибка загрузки способов оплаты: {e}")
        return ["ИП", "Перевод", "Наличные"]  # Fallback значения


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

        logger.info(
            f"🔍 Поиск цены для: product_type='{product_type}', width='{width}', size='{size}', color_type='{color_type}', color='{color}'"
        )

        # Исправляем значение 'None' на пустую строку
        if size == "None":
            size = ""
        if width == "None":
            width = ""

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
            logger.info(
                f"📋 Сравниваем с каталогом: '{catalog_product_type}', '{catalog_width}', '{catalog_size}', '{catalog_color_type}', '{catalog_color}'"
            )

            # Проверяем соответствие всех параметров
            type_match = catalog_product_type == norm_product_type
            width_match = (not norm_width) or (catalog_width == norm_width)
            size_match = (not norm_size) or (catalog_size == norm_size)
            color_type_match = catalog_color_type == norm_color_type
            color_match = catalog_color == norm_color

            logger.info(
                f"   Совпадения: Тип={type_match}, Ширина={width_match}, Размер={size_match}, ТипРасцветки={color_type_match}, Расцветка={color_match}"
            )

            if (
                type_match
                and width_match
                and size_match
                and color_type_match
                and color_match
                and catalog_price
            ):

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

            if (
                catalog_product_type == norm_product_type
                and catalog_color_type == norm_color_type
                and catalog_color == norm_color
                and catalog_price
            ):

                try:
                    price_value = float(clean_numeric_value(catalog_price))
                    logger.info(
                        f"⚠️ Найдена цена по упрощенным параметрам: {price_value} руб."
                    )
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

            if (
                catalog_product_type == norm_product_type
                and catalog_color == norm_color
                and catalog_price
            ):

                try:
                    price_value = float(clean_numeric_value(catalog_price))
                    logger.info(
                        f"⚠️ Найдена цена только по типу и расцветке: {price_value} руб."
                    )
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
                logger.info(
                    f"Строка {i+1}: Тип='{row[2]}', Ширина='{row[3]}', Размер='{row[4]}', ТипРасцветки='{row[5]}', Расцветка='{row[6]}', Цена='{row[7]}'"
                )

        return 0

    except Exception as e:
        logger.error(f"❌ Ошибка поиска цены: {e}", exc_info=True)
        return 0


def get_sales_data():
    """Получает данные о продажах из Google Таблицы"""
    try:
        sheet = get_google_sheet_cached()
        all_data = sheet.get_all_values()
        
        # Пропускаем заголовок
        sales_data = []
        for row in all_data[1:]:
            if len(row) >= 11:  # Проверяем, что строка содержит все необходимые колонки
                sales_data.append({
                    'channel': row[0],
                    'product_type': row[1],
                    'width': row[2],
                    'size': row[3],
                    'color_type': row[4],
                    'color': row[5],
                    'quantity': int(row[6]) if row[6] and row[6].isdigit() else 0,
                    'price': float(clean_numeric_value(row[7])) if row[7] else 0,
                    'total_amount': float(clean_numeric_value(row[8])) if row[8] else 0,
                    'payment_method': row[9],
                    'date': row[10]
                })
        
        return sales_data
    except Exception as e:
        logger.error(f"❌ Ошибка получения данных о продажах: {e}")
        return []


def generate_channel_report(sales_data, period_days=30):
    """Генерирует отчет по каналам продаж"""
    try:
        # Фильтруем данные по периоду
        cutoff_date = datetime.now() - timedelta(days=period_days)
        filtered_data = [
            sale for sale in sales_data 
            if sale['date'] and datetime.strptime(sale['date'], '%d.%m.%Y') >= cutoff_date
        ]
        
        # Группируем по каналам
        channel_stats = {}
        for sale in filtered_data:
            channel = sale['channel']
            if channel not in channel_stats:
                channel_stats[channel] = {
                    'total_sales': 0,
                    'total_amount': 0,
                    'count': 0
                }
            
            channel_stats[channel]['total_sales'] += sale['quantity']
            channel_stats[channel]['total_amount'] += sale['total_amount']
            channel_stats[channel]['count'] += 1
        
        # Формируем отчет
        report_lines = [f"📊 *ОТЧЕТ ПО КАНАЛАМ ПРОДАЖ (за {period_days} дней)*\n"]
        
        # Сортируем по убыванию общей суммы
        sorted_channels = sorted(
            channel_stats.items(), 
            key=lambda x: x[1]['total_amount'], 
            reverse=True
        )
        
        for channel, stats in sorted_channels:
            report_lines.append(
                f"\n📈 *{channel}:*\n"
                f"   • Продаж: {stats['count']}\n"
                f"   • Товаров: {stats['total_sales']} шт.\n"
                f"   • Сумма: {stats['total_amount']:,.2f} руб.\n"
                f"   • Средний чек: {stats['total_amount']/stats['count']:,.2f} руб."
            )
        
        # Итоги
        total_sales = sum(stats['total_sales'] for stats in channel_stats.values())
        total_amount = sum(stats['total_amount'] for stats in channel_stats.values())
        total_count = sum(stats['count'] for stats in channel_stats.values())
        
        report_lines.append(
            f"\n💰 *ИТОГО:*\n"
            f"   • Всего продаж: {total_count}\n"
            f"   • Всего товаров: {total_sales} шт.\n"
            f"   • Общая сумма: {total_amount:,.2f} руб.\n"
            f"   • Средний чек: {total_amount/total_count:,.2f} руб." if total_count > 0 else "   • Средний чек: 0 руб."
        )
        
        return "\n".join(report_lines)
        
    except Exception as e:
        logger.error(f"❌ Ошибка генерации отчета по каналам: {e}")
        return "❌ Ошибка генерации отчета"


def generate_product_report(sales_data, period_days=30):
    """Генерирует отчет по типам товаров"""
    try:
        # Фильтруем данные по периоду
        cutoff_date = datetime.now() - timedelta(days=period_days)
        filtered_data = [
            sale for sale in sales_data 
            if sale['date'] and datetime.strptime(sale['date'], '%d.%m.%Y') >= cutoff_date
        ]
        
        # Группируем по типам товаров
        product_stats = {}
        for sale in filtered_data:
            product_type = sale['product_type']
            if product_type not in product_stats:
                product_stats[product_type] = {
                    'total_sales': 0,
                    'total_amount': 0,
                    'count': 0
                }
            
            product_stats[product_type]['total_sales'] += sale['quantity']
            product_stats[product_type]['total_amount'] += sale['total_amount']
            product_stats[product_type]['count'] += 1
        
        # Формируем отчет
        report_lines = [f"📦 *ОТЧЕТ ПО ТИПАМ ТОВАРОВ (за {period_days} дней)*\n"]
        
        # Сортируем по убыванию общей суммы
        sorted_products = sorted(
            product_stats.items(), 
            key=lambda x: x[1]['total_amount'], 
            reverse=True
        )
        
        for product_type, stats in sorted_products:
            report_lines.append(
                f"\n🏷️ *{product_type}:*\n"
                f"   • Продаж: {stats['count']}\n"
                f"   • Товаров: {stats['total_sales']} шт.\n"
                f"   • Сумма: {stats['total_amount']:,.2f} руб.\n"
                f"   • Средняя цена: {stats['total_amount']/stats['total_sales']:,.2f} руб." if stats['total_sales'] > 0 else "   • Средняя цена: 0 руб."
            )
        
        # Итоги
        total_sales = sum(stats['total_sales'] for stats in product_stats.values())
        total_amount = sum(stats['total_amount'] for stats in product_stats.values())
        total_count = sum(stats['count'] for stats in product_stats.values())
        
        report_lines.append(
            f"\n💰 *ИТОГО:*\n"
            f"   • Всего продаж: {total_count}\n"
            f"   • Всего товаров: {total_sales} шт.\n"
            f"   • Общая сумма: {total_amount:,.2f} руб.\n"
            f"   • Средний чек: {total_amount/total_count:,.2f} руб." if total_count > 0 else "   • Средний чек: 0 руб."
        )
        
        return "\n".join(report_lines)
        
    except Exception as e:
        logger.error(f"❌ Ошибка генерации отчета по товарам: {e}")
        return "❌ Ошибка генерации отчета"


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


def payment_methods_keyboard():
    """Клавиатура со способами оплаты"""
    try:
        payment_methods = get_payment_methods_from_sheet()
        keyboard = []

        for method in payment_methods:
            keyboard.append(
                [InlineKeyboardButton(method, callback_data=f"payment_{method}")]
            )

        keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel")])
        return InlineKeyboardMarkup(keyboard)

    except Exception as e:
        logger.error(f"❌ Ошибка создания клавиатуры способов оплаты: {e}")
        # Fallback значения
        keyboard = [
            [InlineKeyboardButton("ИП", callback_data="payment_ИП")],
            [InlineKeyboardButton("Перевод", callback_data="payment_Перевод")],
            [InlineKeyboardButton("Наличные", callback_data="payment_Наличные")],
            [InlineKeyboardButton("❌ Отмена", callback_data="cancel")]
        ]
        return InlineKeyboardMarkup(keyboard)


def report_types_keyboard():
    """Клавиатура с типами отчетов"""
    keyboard = [
        [InlineKeyboardButton("📊 По каналам продаж", callback_data="report_channels")],
        [InlineKeyboardButton("📦 По типам товаров", callback_data="report_products")],
        [InlineKeyboardButton("❌ Отмена", callback_data="cancel")]
    ]
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
    """Обработчик команды /add для нового процесса"""
    user_id = update.message.from_user.id

    # Очищаем предыдущее состояние пользователя в БД
    try:
        with get_db_cursor() as cur:
            cur.execute(
                "DELETE FROM user_states WHERE user_id = %s",
                (user_id,),
            )
    except Exception as e:
        logger.error(f"❌ Ошибка очистки состояния пользователя {user_id}: {e}")

    # Отправляем сообщение с выбором канала продаж
    await update.message.reply_text(
        "📊 Выберите канал продаж:",
        reply_markup=sales_channels_keyboard(),
    )


async def generate_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /report для генерации отчетов"""
    await update.message.reply_text(
        "📈 Выберите тип отчета:",
        reply_markup=report_types_keyboard(),
    )


# ==================== ОБРАБОТЧИКИ CALLBACK ====================
async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик всех callback-запросов"""
    query = update.callback_query
    await query.answer()

    user_id = query.from_user.id
    callback_data = query.data

    logger.info(f"📨 Callback от пользователя {user_id}: {callback_data}")

    # Обработка отмены
    if callback_data == "cancel":
        try:
            with get_db_cursor() as cur:
                cur.execute(
                    "DELETE FROM user_states WHERE user_id = %s",
                    (user_id,),
                )
        except Exception as e:
            logger.error(f"❌ Ошибка удаления состояния пользователя {user_id}: {e}")

        await query.edit_message_text("❌ Операция отменена.")
        return

    # Обработка выбора канала продаж
    if not callback_data.startswith(("type_", "width_", "size_", "colortype_", "color_", "payment_", "report_")):
        try:
            with get_db_cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO user_states (user_id, channel)
                    VALUES (%s, %s)
                    ON CONFLICT (user_id) 
                    DO UPDATE SET channel = EXCLUDED.channel
                    """,
                    (user_id, callback_data),
                )
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения канала пользователя {user_id}: {e}")
            await query.edit_message_text("❌ Ошибка сохранения. Попробуйте снова.")
            return

        await query.edit_message_text(
            f"📊 Канал продаж: {callback_data}\n\n🏷️ Выберите тип товара:",
            reply_markup=product_types_keyboard(),
        )
        return

    # Обработка выбора типа товара
    if callback_data.startswith("type_"):
        product_type = callback_data[5:]  # Убираем префикс "type_"

        try:
            with get_db_cursor() as cur:
                cur.execute(
                    """
                    UPDATE user_states 
                    SET product_type = %s
                    WHERE user_id = %s
                    """,
                    (product_type, user_id),
                )
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения типа товара пользователя {user_id}: {e}")
            await query.edit_message_text("❌ Ошибка сохранения. Попробуйте снова.")
            return

        # Получаем информацию о товаре из справочника
        ref_data = get_reference_data()
        product_info = next(
            (p for p in ref_data["product_types"] if p["type"] == product_type), None
        )

        if product_info and product_info["has_width"]:
            await query.edit_message_text(
                f"🏷️ Тип товара: {product_type}\n\n📏 Выберите ширину стропы:",
                reply_markup=widths_keyboard(),
            )
        else:
            # Для товаров без ширины (Лежанка, Бусы) пропускаем выбор ширины и размера
            try:
                with get_db_cursor() as cur:
                    cur.execute(
                        """
                        UPDATE user_states 
                        SET width = 'None', size = 'None'
                        WHERE user_id = %s
                        """,
                        (user_id,),
                    )
            except Exception as e:
                logger.error(f"❌ Ошибка обновления состояния пользователя {user_id}: {e}")

            await query.edit_message_text(
                f"🏷️ Тип товара: {product_type}\n\n🎨 Выберите тип расцветки:",
                reply_markup=color_types_keyboard(),
            )
        return

    # Обработка выбора ширины
    if callback_data.startswith("width_"):
        width = callback_data[6:]  # Убираем префикс "width_"

        try:
            with get_db_cursor() as cur:
                cur.execute(
                    """
                    UPDATE user_states 
                    SET width = %s
                    WHERE user_id = %s
                    """,
                    (width, user_id),
                )
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения ширины пользователя {user_id}: {e}")
            await query.edit_message_text("❌ Ошибка сохранения. Попробуйте снова.")
            return

        await query.edit_message_text(
            f"📏 Ширина стропы: {width}\n\n📐 Выберите размер:",
            reply_markup=sizes_keyboard(width),
        )
        return

    # Обработка выбора размера
    if callback_data.startswith("size_"):
        size = callback_data[5:]  # Убираем префикс "size_"

        try:
            with get_db_cursor() as cur:
                cur.execute(
                    """
                    UPDATE user_states 
                    SET size = %s
                    WHERE user_id = %s
                    """,
                    (size, user_id),
                )
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения размера пользователя {user_id}: {e}")
            await query.edit_message_text("❌ Ошибка сохранения. Попробуйте снова.")
            return

        await query.edit_message_text(
            f"📐 Размер: {size}\n\n🎨 Выберите тип расцветки:",
            reply_markup=color_types_keyboard(),
        )
        return

    # Обработка выбора типа расцветки
    if callback_data.startswith("colortype_"):
        color_type = callback_data[10:]  # Убираем префикс "colortype_"

        try:
            with get_db_cursor() as cur:
                cur.execute(
                    """
                    UPDATE user_states 
                    SET color_type = %s
                    WHERE user_id = %s
                    """,
                    (color_type, user_id),
                )
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения типа расцветки пользователя {user_id}: {e}")
            await query.edit_message_text("❌ Ошибка сохранения. Попробуйте снова.")
            return

        # Получаем информацию о товаре из состояния пользователя
        try:
            with get_db_cursor() as cur:
                cur.execute(
                    "SELECT product_type FROM user_states WHERE user_id = %s",
                    (user_id,),
                )
                user_state = cur.fetchone()
                product_type = user_state["product_type"] if user_state else None
        except Exception as e:
            logger.error(f"❌ Ошибка получения состояния пользователя {user_id}: {e}")
            await query.edit_message_text("❌ Ошибка. Попробуйте снова.")
            return

        # Для Лежанки и Бус используем все расцветки
        if product_type in ["Лежанка", "Бусы"]:
            await query.edit_message_text(
                f"🎨 Тип расцветки: {color_type}\n\n🌈 Выберите расцветку:",
                reply_markup=all_colors_keyboard(),
            )
        else:
            await query.edit_message_text(
                f"🎨 Тип расцветки: {color_type}\n\n🌈 Выберите расцветку:",
                reply_markup=colors_keyboard(color_type),
            )
        return

    # Обработка выбора расцветки
    if callback_data.startswith("color_"):
        color = callback_data[6:]  # Убираем префикс "color_"

        try:
            with get_db_cursor() as cur:
                cur.execute(
                    """
                    UPDATE user_states 
                    SET color = %s
                    WHERE user_id = %s
                    """,
                    (color, user_id),
                )
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения расцветки пользователя {user_id}: {e}")
            await query.edit_message_text("❌ Ошибка сохранения. Попробуйте снова.")
            return

        await query.edit_message_text(
            f"🌈 Расцветка: {color}\n\n💳 Выберите способ оплаты:",
            reply_markup=payment_methods_keyboard(),
        )
        return

    # Обработка выбора способа оплаты
    if callback_data.startswith("payment_"):
        payment_method = callback_data[8:]  # Убираем префикс "payment_"

        try:
            with get_db_cursor() as cur:
                cur.execute(
                    """
                    UPDATE user_states 
                    SET payment_method = %s
                    WHERE user_id = %s
                    """,
                    (payment_method, user_id),
                )
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения способа оплаты пользователя {user_id}: {e}")
            await query.edit_message_text("❌ Ошибка сохранения. Попробуйте снова.")
            return

        # Получаем все данные пользователя для расчета цены
        try:
            with get_db_cursor() as cur:
                cur.execute(
                    "SELECT * FROM user_states WHERE user_id = %s",
                    (user_id,),
                )
                user_state = cur.fetchone()
        except Exception as e:
            logger.error(f"❌ Ошибка получения состояния пользователя {user_id}: {e}")
            await query.edit_message_text("❌ Ошибка. Попробуйте снова.")
            return

        if user_state:
            # Рассчитываем цену
            price = get_product_price_from_catalog(
                user_state["product_type"],
                user_state["width"],
                user_state["size"],
                user_state["color_type"],
                user_state["color"],
            )

            # Сохраняем цену в контексте для использования в следующем шаге
            context.user_data["current_price"] = price

            await query.edit_message_text(
                f"💳 Способ оплаты: {payment_method}\n\n"
                f"💰 Цена за единицу: {price:,.2f} руб.\n\n"
                f"🔢 Введите количество товара:"
            )
        return

    # Обработка выбора типа отчета
    if callback_data.startswith("report_"):
        report_type = callback_data[7:]  # Убираем префикс "report_"
        
        sales_data = get_sales_data()
        
        if report_type == "channels":
            report_text = generate_channel_report(sales_data)
        elif report_type == "products":
            report_text = generate_product_report(sales_data)
        else:
            report_text = "❌ Неизвестный тип отчета"
        
        await query.edit_message_text(report_text, parse_mode="Markdown")
        return


# ==================== ОБРАБОТЧИКИ СООБЩЕНИЙ ====================
async def handle_quantity_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик ввода количества товара"""
    user_id = update.message.from_user.id
    quantity_text = update.message.text

    # Проверяем, что введено число
    try:
        quantity = int(quantity_text)
        if quantity <= 0:
            await update.message.reply_text("❌ Количество должно быть больше 0.")
            return
    except ValueError:
        await update.message.reply_text("❌ Пожалуйста, введите целое число.")
        return

    # Получаем данные пользователя из БД
    try:
        with get_db_cursor() as cur:
            cur.execute(
                "SELECT * FROM user_states WHERE user_id = %s",
                (user_id,),
            )
            user_state = cur.fetchone()
    except Exception as e:
        logger.error(f"❌ Ошибка получения состояния пользователя {user_id}: {e}")
        await update.message.reply_text("❌ Ошибка. Попробуйте снова с команды /add.")
        return

    if not user_state:
        await update.message.reply_text("❌ Данные не найдены. Начните с команды /add.")
        return

    # Получаем цену из контекста
    price = context.user_data.get("current_price", 0)
    total_amount = price * quantity

    # Записываем данные в Google Таблицу
    try:
        sheet = get_google_sheet_cached()
        current_date = datetime.now().strftime("%d.%m.%Y")

        # Подготавливаем данные для записи
        row_data = [
            user_state["channel"],
            user_state["product_type"],
            user_state["width"] if user_state["width"] else "",
            user_state["size"] if user_state["size"] else "",
            user_state["color_type"],
            user_state["color"],
            str(quantity),
            str(price),
            str(total_amount),
            user_state["payment_method"],
            current_date,
        ]

        # Добавляем новую строку
        sheet.append_row(row_data)

        logger.info(f"✅ Данные записаны в Google Таблицу: {row_data}")

        # Формируем сообщение об успехе
        success_message = f"""
✅ *Запись успешно добавлена!*

📊 Канал продаж: {user_state['channel']}
🏷️ Товар: {user_state['product_type']}
📏 Ширина: {user_state['width'] if user_state['width'] else 'Не указана'}
📐 Размер: {user_state['size'] if user_state['size'] else 'Не указан'}
🎨 Тип расцветки: {user_state['color_type']}
🌈 Расцветка: {user_state['color']}
💳 Способ оплаты: {user_state['payment_method']}
🔢 Количество: {quantity} шт.
💰 Цена за единицу: {price:,.2f} руб.
💵 Общая сумма: {total_amount:,.2f} руб.
📅 Дата: {current_date}
"""

        await update.message.reply_text(success_message, parse_mode="Markdown")

        # Очищаем состояние пользователя
        try:
            with get_db_cursor() as cur:
                cur.execute(
                    "DELETE FROM user_states WHERE user_id = %s",
                    (user_id,),
                )
        except Exception as e:
            logger.error(f"❌ Ошибка очистки состояния пользователя {user_id}: {e}")

    except Exception as e:
        logger.error(f"❌ Ошибка записи в Google Таблицу: {e}")
        await update.message.reply_text(
            "❌ Ошибка записи данных. Попробуйте снова с команды /add."
        )


# ==================== ОСНОВНАЯ ФУНКЦИЯ ====================
def main():
    """Основная функция бота"""
    logger.info("🚀 Запуск бота...")

    # Инициализация БД
    init_db()

    # Создаем Application
    application = Application.builder().token(BOT_TOKEN).build()

    # Добавляем обработчики команд
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("add", add_entry))
    application.add_handler(CommandHandler("report", generate_report))

    # Добавляем обработчики callback-запросов
    application.add_handler(CallbackQueryHandler(handle_callback))

    # Добавляем обработчик текстовых сообщений (для ввода количества)
    application.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, handle_quantity_input)
    )

    # Запускаем бота
    logger.info("✅ Бот запущен и готов к работе!")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()