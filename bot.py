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
EXPENSES_SHEET_NAME = "Расходы"
EXPENSE_CATEGORIES_SHEET_NAME = "Категории расходов"

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
                if len(row) >= 9:  # Обновлено для учета длины
                    logger.info(f"Строка {i+1}: {row[:9]}")  # Первые 9 колонок
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

            for col_idx, value in enumerate(row[:9]):  # Первые 9 колонок
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
                    length VARCHAR(20),
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
            if current_section == "product_types" and len(row) >= 4:
                if row[0] and row[0] != "Тип товара":  # Пропускаем заголовок
                    reference_data["product_types"].append(
                        {
                            "type": row[0].strip(),
                            "has_width": row[1].strip().lower() == "да",
                            "has_size": row[2].strip().lower() == "да",
                            "has_length": row[3].strip().lower() == "да",
                        }
                    )

            elif current_section == "widths" and len(row) >= 3:
                if row[0] and row[0] != "Ширина":  # Пропускаем заголовок
                    available_sizes = (
                        [s.strip() for s in row[1].split(",")] if row[1] else []
                    )
                    available_lengths = (
                        [l.strip() for l in row[2].split(",")] if row[2] else []
                    )
                    reference_data["widths"].append(
                        {
                            "width": row[0].strip(),
                            "available_sizes": available_sizes,
                            "available_lengths": available_lengths,
                        }
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


def get_product_price_from_catalog(
    product_type, width, size, length, color_type, color
):
    """Находит цену товара в каталоге по параметрам"""
    try:
        sheet = get_google_sheet_cached()
        catalog_sheet = sheet.spreadsheet.worksheet(CATALOG_SHEET_NAME)
        all_data = catalog_sheet.get_all_values()

        logger.info(
            f"🔍 Поиск цены для: product_type='{product_type}', width='{width}', size='{size}', length='{length}', color_type='{color_type}', color='{color}'"
        )

        # Исправляем значение 'None' на пустую строку
        if size == "None":
            size = ""
        if width == "None":
            width = ""
        if length == "None":
            length = ""

        # Функция для нормализации сравнения (приводим к нижнему регистру и убираем пробелы)
        def normalize(text):
            return str(text).lower().strip() if text else ""

        norm_product_type = normalize(product_type)
        norm_width = normalize(width)
        norm_size = normalize(size)
        norm_length = normalize(length)
        norm_color_type = normalize(color_type)
        norm_color = normalize(color)

        # Пропускаем заголовок
        for i, row in enumerate(all_data[1:], start=2):
            if len(row) < 9:  # Теперь 9 колонок с учетом длины
                continue

            # Получаем значения из каталога
            catalog_product_type = normalize(row[2]) if len(row) > 2 else ""
            catalog_width = normalize(row[3]) if len(row) > 3 else ""
            catalog_size = normalize(row[4]) if len(row) > 4 else ""
            catalog_length = normalize(row[5]) if len(row) > 5 else ""
            catalog_color_type = normalize(row[6]) if len(row) > 6 else ""
            catalog_color = normalize(row[7]) if len(row) > 7 else ""
            catalog_price = row[8].strip() if len(row) > 8 else ""

            # Логируем для отладки
            logger.info(
                f"📋 Сравниваем с каталогом: '{catalog_product_type}', '{catalog_width}', '{catalog_size}', '{catalog_length}', '{catalog_color_type}', '{catalog_color}'"
            )

            # Проверяем соответствие всех параметров
            type_match = catalog_product_type == norm_product_type
            width_match = (not norm_width) or (catalog_width == norm_width)
            size_match = (not norm_size) or (catalog_size == norm_size)
            length_match = (not norm_length) or (catalog_length == norm_length)
            color_type_match = catalog_color_type == norm_color_type
            color_match = catalog_color == norm_color

            logger.info(
                f"   Совпадения: Тип={type_match}, Ширина={width_match}, Размер={size_match}, Длина={length_match}, ТипРасцветки={color_type_match}, Расцветка={color_match}"
            )

            if (
                type_match
                and width_match
                and size_match
                and length_match
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
            if len(row) < 9:
                continue

            catalog_product_type = normalize(row[2]) if len(row) > 2 else ""
            catalog_color_type = normalize(row[6]) if len(row) > 6 else ""
            catalog_color = normalize(row[7]) if len(row) > 7 else ""
            catalog_price = row[8].strip() if len(row) > 8 else ""

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
            if len(row) < 9:
                continue

            catalog_product_type = normalize(row[2]) if len(row) > 2 else ""
            catalog_color = normalize(row[7]) if len(row) > 7 else ""
            catalog_price = row[8].strip() if len(row) > 8 else ""

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
            elif len(row) >= 9:
                logger.info(
                    f"Строка {i+1}: Тип='{row[2]}', Ширина='{row[3]}', Размер='{row[4]}', Длина='{row[5]}', ТипРасцветки='{row[6]}', Расцветка='{row[7]}', Цена='{row[8]}'"
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
            if len(row) >= 12:  # Проверяем, что строка содержит все необходимые колонки
                sales_data.append(
                    {
                        "channel": row[0],
                        "product_type": row[1],
                        "width": row[2],
                        "size": row[3],
                        "length": row[4],
                        "color_type": row[5],
                        "color": row[6],
                        "quantity": int(row[7]) if row[7] and row[7].isdigit() else 0,
                        "price": float(clean_numeric_value(row[8])) if row[8] else 0,
                        "total_amount": (
                            float(clean_numeric_value(row[9])) if row[9] else 0
                        ),
                        "payment_method": row[10],
                        "date": row[11],
                    }
                )

        return sales_data
    except Exception as e:
        logger.error(f"❌ Ошибка получения данных о продажах: {e}")
        return []

def get_expenses_data():
    """Получает данные о расходах из Google Таблицы"""
    try:
        sheet = get_google_sheet_cached()
        
        try:
            expenses_sheet = sheet.spreadsheet.worksheet(EXPENSES_SHEET_NAME)
            all_data = expenses_sheet.get_all_values()
        except Exception as e:
            logger.error(f"❌ Лист '{EXPENSES_SHEET_NAME}' не найден: {e}")
            return []

        # Пропускаем заголовок
        expenses_data = []
        for row in all_data[1:]:
            if len(row) >= 4:  # Проверяем, что строка содержит все необходимые колонки
                expenses_data.append(
                    {
                        "category": row[0],
                        "amount": float(clean_numeric_value(row[1])) if row[1] else 0,
                        "date": row[2],
                        "comment": row[3] if len(row) > 3 else ""
                    }
                )

        return expenses_data
    except Exception as e:
        logger.error(f"❌ Ошибка получения данных о расходах: {e}")
        return []

def generate_channel_report(sales_data, period_days=30):
    """Генерирует отчет по каналам продаж"""
    try:
        # Фильтруем данные по периоду
        cutoff_date = datetime.now() - timedelta(days=period_days)
        filtered_data = [
            sale
            for sale in sales_data
            if sale["date"]
            and datetime.strptime(sale["date"], "%d.%m.%Y") >= cutoff_date
        ]

        # Группируем по каналам
        channel_stats = {}
        for sale in filtered_data:
            channel = sale["channel"]
            if channel not in channel_stats:
                channel_stats[channel] = {
                    "total_sales": 0,
                    "total_amount": 0,
                    "count": 0,
                }

            channel_stats[channel]["total_sales"] += sale["quantity"]
            channel_stats[channel]["total_amount"] += sale["total_amount"]
            channel_stats[channel]["count"] += 1

        # Формируем отчет
        report_lines = [f"📊 *ОТЧЕТ ПО КАНАЛАМ ПРОДАЖ (за {period_days} дней)*\n"]

        # Сортируем по убыванию общей суммы
        sorted_channels = sorted(
            channel_stats.items(), key=lambda x: x[1]["total_amount"], reverse=True
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
        total_sales = sum(stats["total_sales"] for stats in channel_stats.values())
        total_amount = sum(stats["total_amount"] for stats in channel_stats.values())
        total_count = sum(stats["count"] for stats in channel_stats.values())

        report_lines.append(
            f"\n💰 *ИТОГО:*\n"
            f"   • Всего продаж: {total_count}\n"
            f"   • Всего товаров: {total_sales} шт.\n"
            f"   • Общая сумма: {total_amount:,.2f} руб.\n"
            f"   • Средний чек: {total_amount/total_count:,.2f} руб."
            if total_count > 0
            else "   • Средний чек: 0 руб."
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
            sale
            for sale in sales_data
            if sale["date"]
            and datetime.strptime(sale["date"], "%d.%m.%Y") >= cutoff_date
        ]

        # Гroupпируем по типам товаров
        product_stats = {}
        for sale in filtered_data:
            product_type = sale["product_type"]
            if product_type not in product_stats:
                product_stats[product_type] = {
                    "total_sales": 0,
                    "total_amount": 0,
                    "count": 0,
                }

            product_stats[product_type]["total_sales"] += sale["quantity"]
            product_stats[product_type]["total_amount"] += sale["total_amount"]
            product_stats[product_type]["count"] += 1

        # Формируем отчет
        report_lines = [f"📦 *ОТЧЕТ ПО ТИПАМ ТОВАРОВ (за {period_days} дней)*\n"]

        # Сортируем по убыванию общей суммы
        sorted_products = sorted(
            product_stats.items(), key=lambda x: x[1]["total_amount"], reverse=True
        )

        for product_type, stats in sorted_products:
            report_lines.append(
                f"\n🏷️ *{product_type}:*\n"
                f"   • Продаж: {stats['count']}\n"
                f"   • Товаров: {stats['total_sales']} шт.\n"
                f"   • Сумма: {stats['total_amount']:,.2f} руб.\n"
                f"   • Средняя цена: {stats['total_amount']/stats['total_sales']:,.2f} руб."
                if stats["total_sales"] > 0
                else "   • Средняя цена: 0 руб."
            )

        # Итоги
        total_sales = sum(stats["total_sales"] for stats in product_stats.values())
        total_amount = sum(stats["total_amount"] for stats in product_stats.values())
        total_count = sum(stats["count"] for stats in product_stats.values())

        report_lines.append(
            f"\n💰 *ИТОГО:*\n"
            f"   • Всего продаж: {total_count}\n"
            f"   • Всего товаров: {total_sales} шт.\n"
            f"   • Общая сумма: {total_amount:,.2f} руб.\n"
            f"   • Средний чек: {total_amount/total_count:,.2f} руб."
            if total_count > 0
            else "   • Средний чек: 0 руб."
        )

        return "\n".join(report_lines)

    except Exception as e:
        logger.error(f"❌ Ошибка генерации отчета по товарам: {e}")
        return "❌ Ошибка генерации отчета"

def generate_expenses_report(expenses_data, period_days=30):
    """Генерирует отчет по расходам"""
    try:
        # Фильтруем данные по периоду
        cutoff_date = datetime.now() - timedelta(days=period_days)
        filtered_data = [
            expense
            for expense in expenses_data
            if expense["date"]
            and datetime.strptime(expense["date"], "%d.%m.%Y") >= cutoff_date
        ]

        # Группируем по категориям
        category_stats = {}
        for expense in filtered_data:
            category = expense["category"]
            if category not in category_stats:
                category_stats[category] = {
                    "total_amount": 0,
                    "count": 0,
                }

            category_stats[category]["total_amount"] += expense["amount"]
            category_stats[category]["count"] += 1

        # Формируем отчет
        report_lines = [f"💰 *ОТЧЕТ ПО РАСХОДАМ (за {period_days} дней)*\n"]

        # Сортируем по убыванию общей суммы
        sorted_categories = sorted(
            category_stats.items(), key=lambda x: x[1]["total_amount"], reverse=True
        )

        for category, stats in sorted_categories:
            report_lines.append(
                f"\n📊 *{category}:*\n"
                f"   • Количество: {stats['count']}\n"
                f"   • Сумма: {stats['total_amount']:,.2f} руб.\n"
                f"   • Средний расход: {stats['total_amount']/stats['count']:,.2f} руб."
                if stats["count"] > 0
                else "   • Средний расход: 0 руб."
            )

        # Итоги
        total_amount = sum(stats["total_amount"] for stats in category_stats.values())
        total_count = sum(stats["count"] for stats in category_stats.values())

        report_lines.append(
            f"\n💸 *ИТОГО:*\n"
            f"   • Всего расходов: {total_count}\n"
            f"   • Общая сумма: {total_amount:,.2f} руб.\n"
            f"   • Средний расход: {total_amount/total_count:,.2f} руб."
            if total_count > 0
            else "   • Средний расход: 0 руб."
        )

        return "\n".join(report_lines)

    except Exception as e:
        logger.error(f"❌ Ошибка генерации отчета по расходам: {e}")
        return "❌ Ошибка генерации отчета по расходам"

@lru_cache(maxsize=1)
def get_expense_categories_from_sheet():
    """Загружает список категорий расходов из Google Таблицы с кешированием"""
    try:
        logger.info("🔄 Загружаю список категорий расходов из Google Таблицы...")
        sheet = get_google_sheet_cached()

        try:
            categories_sheet = sheet.spreadsheet.worksheet(EXPENSE_CATEGORIES_SHEET_NAME)
            logger.info("✅ Лист 'Категории расходов' найден")
        except Exception as e:
            logger.error(f"❌ Лист 'Категории расходов' не найден: {e}")
            return []

        all_data = categories_sheet.get_all_values()
        logger.info(f"📊 Получено строк с листа 'Категории расходов': {len(all_data)}")

        # Пропускаем заголовок
        categories_data = all_data[1:] if len(all_data) > 1 else []

        # Формируем список категорий
        categories_list = []
        for row in categories_data:
            if len(row) >= 2 and row[1]:  # Берем значение из колонки "Категория"
                categories_list.append(row[1].strip())

        logger.info(f"✅ Загружено {len(categories_list)} категорий расходов: {categories_list}")
        return categories_list

    except Exception as e:
        logger.error(f"❌ Ошибка загрузки категорий расходов: {e}")
        return []   


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


def lengths_keyboard(selected_width):
    """Клавиатура с длинами для выбранной ширины"""
    try:
        ref_data = get_reference_data()
        keyboard = []

        # Находим доступные длины для выбранной ширины
        width_data = next(
            (w for w in ref_data["widths"] if w["width"] == selected_width), None
        )

        if width_data:
            for length in width_data["available_lengths"]:
                keyboard.append(
                    [InlineKeyboardButton(length, callback_data=f"length_{length}")]
                )

        keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel")])
        return InlineKeyboardMarkup(keyboard)

    except Exception as e:
        logger.error(f"❌ Ошибка создания клавиатуры длин: {e}")
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


def payment_methods_keyboard():
    """Клавиатура со способами оплаты"""
    try:
        payment_methods = get_payment_methods_from_sheet()
        keyboard = []

        for method in payment_methods:
            keyboard.append(
                [InlineKeyboardButton(method, callback_data=f"payment_{method}")]
            )

        # Добавляем кнопку для ручного ввода цены
        keyboard.append(
            [
                InlineKeyboardButton(
                    "✏️ Ввести цену вручную", callback_data="manual_price"
                )
            ]
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
            [
                InlineKeyboardButton(
                    "✏️ Ввести цену вручную", callback_data="manual_price"
                )
            ],
            [InlineKeyboardButton("❌ Отмена", callback_data="cancel")],
        ]
        return InlineKeyboardMarkup(keyboard)


def report_types_keyboard():
    """Клавиатура с типами отчетов"""
    keyboard = [
        [InlineKeyboardButton("📊 По каналам продаж", callback_data="report_channels")],
        [InlineKeyboardButton("📦 По типам товаров", callback_data="report_products")],
        [InlineKeyboardButton("💰 По расходам", callback_data="report_expenses")],
        [InlineKeyboardButton("❌ Отмена", callback_data="cancel")],
    ]
    return InlineKeyboardMarkup(keyboard)

def expense_categories_keyboard():
    """Клавиатура с категориями расходов"""
    try:
        categories = get_expense_categories_from_sheet()
        keyboard = []

        # Создаем кнопки (по 2 в ряд)
        for i in range(0, len(categories), 2):
            row = []
            row.append(InlineKeyboardButton(categories[i], callback_data=f"expense_cat_{categories[i]}"))
            
            if i + 1 < len(categories):
                row.append(InlineKeyboardButton(categories[i + 1], callback_data=f"expense_cat_{categories[i + 1]}"))
            
            keyboard.append(row)

        # Добавляем кнопку "Отмена"
        keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel")])

        return InlineKeyboardMarkup(keyboard)
    except Exception as e:
        logger.error(f"❌ Ошибка создания клавиатуры категорий расходов: {e}")
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
                "DELETE FROM user_states WHERE user_id = %s",
                (user_id,),
            )
    except Exception as e:
        logger.error(f"❌ Ошибка очистки состояния пользователя {user_id}: {e}")

    # Запрашиваем канал продаж
    await update.message.reply_text(
        "📺 Выберите канал продаж:",
        reply_markup=sales_channels_keyboard(),
    )


async def generate_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /report"""
    await update.message.reply_text(
        "📊 Выберите тип отчета:",
        reply_markup=report_types_keyboard(),
    )

async def add_expense(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /addexpense для добавления расхода"""
    user_id = update.message.from_user.id
    
    # Очищаем предыдущее состояние расходов
    if 'expense_data' in context.user_data:
        del context.user_data['expense_data']
    
    # Запрашиваем категорию расхода
    await update.message.reply_text(
        "📋 Выберите категорию расхода:",
        reply_markup=expense_categories_keyboard(),
    )

# ==================== ОБРАБОТЧИКИ КНОПОК ====================
async def handle_callback_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик всех callback запросов"""
    query = update.callback_query
    await query.answer()

    user_id = query.from_user.id
    callback_data = query.data

    logger.info(f"🔄 Обработка callback от {user_id}: {callback_data}")

    # Обработка отмены
    if callback_data == "cancel":
        try:
            with get_db_cursor() as cur:
                cur.execute(
                    "DELETE FROM user_states WHERE user_id = %s",
                    (user_id,),
                )
        except Exception as e:
            logger.error(f"❌ Ошибка при отмене для пользователя {user_id}: {e}")

        await query.edit_message_text("❌ Операция отменена.")
        return

    # Обработка отчетов
    if callback_data == "report_channels":
        sales_data = get_sales_data()
        report = generate_channel_report(sales_data)
        await query.edit_message_text(report, parse_mode="Markdown")
        return

    if callback_data == "report_products":
        sales_data = get_sales_data()
        report = generate_product_report(sales_data)
        await query.edit_message_text(report, parse_mode="Markdown")
        return
    
    if callback_data == "report_expenses":
        expenses_data = get_expenses_data()
        report = generate_expenses_report(expenses_data)
        await query.edit_message_text(report, parse_mode="Markdown")
        return
    
    # Обработка выбора категории расхода
    if callback_data.startswith("expense_cat_"):
        category = callback_data.replace("expense_cat_", "")
        
        # Сохраняем категорию в контексте
        context.user_data['expense_data'] = {'category': category}
        
        await query.edit_message_text(
            f"📋 Категория: {category}\n\n"
            f"💵 Введите сумму расхода (например: 1500.50):"
        )
        return

    # Получаем текущее состояние пользователя из БД
    try:
        with get_db_cursor() as cur:
            cur.execute(
                "SELECT * FROM user_states WHERE user_id = %s",
                (user_id,),
            )
            user_state = cur.fetchone()
    except Exception as e:
        logger.error(f"❌ Ошибка получения состояния пользователя {user_id}: {e}")
        await query.edit_message_text("❌ Ошибка. Попробуйте снова /add")
        return

    # Обработка выбора канала продаж
    if not user_state or not user_state.get("channel"):
        # Сохраняем канал в БД
        try:
            with get_db_cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO user_states (user_id, channel)
                    VALUES (%s, %s)
                    ON CONFLICT (user_id) DO UPDATE SET channel = EXCLUDED.channel
                    """,
                    (user_id, callback_data),
                )
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения канала для {user_id}: {e}")
            await query.edit_message_text("❌ Ошибка. Попробуйте снова /add")
            return

        # Запрашиваем тип товара
        await query.edit_message_text(
            "• Выберите тип товара:",
            reply_markup=product_types_keyboard(),
        )
        return

    # Обработка выбора типа товара
    if callback_data.startswith("type_") and not user_state.get("product_type"):
        product_type = callback_data.replace("type_", "")

        # Сохраняем тип товара в БД
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
            logger.error(f"❌ Ошибка сохранения типа товара для {user_id}: {e}")
            await query.edit_message_text("❌ Ошибка. Попробуйте снова /add")
            return

        # Получаем информацию о типе товара из справочника
        ref_data = get_reference_data()
        product_info = next(
            (p for p in ref_data["product_types"] if p["type"] == product_type), None
        )

        if not product_info:
            await query.edit_message_text("❌ Ошибка. Попробуйте снова /add")
            return

        # Проверяем тип товара для пропуска этапов
        if product_type in ["Лежанка", "Бусы"]:
            # Пропускаем выбор ширины и размера, переходим сразу к выбору типа расцветки
            await query.edit_message_text(
                "• Выберите тип расцветки:",
                reply_markup=color_types_keyboard(),
            )
            return

        # Проверяем, нужно ли выбирать ширину
        if product_info["has_width"]:
            await query.edit_message_text(
                "• Выберите ширину строп:",
                reply_markup=widths_keyboard(),
            )
        else:
            # Если ширина не нужна, проверяем другие параметры
            if product_info["has_size"]:
                await query.edit_message_text(
                    "• Выберите размер:",
                    reply_markup=sizes_keyboard(""),
                )
            elif product_info["has_length"]:
                await query.edit_message_text(
                    "• Выберите длину:",
                    reply_markup=lengths_keyboard(""),
                )
            else:
                # Если ни размер, ни длина не нужны, переходим к выбору типа расцветки
                await query.edit_message_text(
                    "• Выберите тип расцветки:",
                    reply_markup=color_types_keyboard(),
                )
        return

    # Обработка выбора ширины
    if callback_data.startswith("width_") and not user_state.get("width"):
        width = callback_data.replace("width_", "")

        # Сохраняем ширину в БД
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
            logger.error(f"❌ Ошибка сохранения ширины для {user_id}: {e}")
            await query.edit_message_text("❌ Ошибка. Попробуйте снова /add")
            return

        # Получаем информацию о типе товара
        try:
            with get_db_cursor() as cur:
                cur.execute(
                    "SELECT product_type FROM user_states WHERE user_id = %s",
                    (user_id,),
                )
                result = cur.fetchone()
                product_type = result["product_type"] if result else None
        except Exception as e:
            logger.error(f"❌ Ошибка получения типа товара для {user_id}: {e}")
            await query.edit_message_text("❌ Ошибка. Попробуйте снова /add")
            return

        # Проверяем, нужно ли выбирать размер или длину для данного типа товара
        ref_data = get_reference_data()
        product_info = next(
            (p for p in ref_data["product_types"] if p["type"] == product_type), None
        )

        if not product_info:
            await query.edit_message_text("❌ Ошибка: тип товара не найден")
            return

        if product_info["has_size"]:
            await query.edit_message_text(
                "• Выберите размер:",
                reply_markup=sizes_keyboard(width),
            )
        elif product_info["has_length"]:
            await query.edit_message_text(
                "• Выберите длину:",
                reply_markup=lengths_keyboard(width),
            )
        else:
            # Если ни размер, ни длина не нужны, переходим к выбору типа расцветки
            await query.edit_message_text(
                "• Выберите тип расцветки:",
                reply_markup=color_types_keyboard(),
            )
        return

    # Обработка выбора размера
    if callback_data.startswith("size_") and not user_state.get("size"):
        size = callback_data.replace("size_", "")

        # Сохраняем размер в БД
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
            logger.error(f"❌ Ошибка сохранения размера для {user_id}: {e}")
            await query.edit_message_text("❌ Ошибка. Попробуйте снова /add")
            return

        await query.edit_message_text(
            "• Выберите тип расцветки:",
            reply_markup=color_types_keyboard(),
        )
        return

    # Обработка выбора длины
    if callback_data.startswith("length_") and not user_state.get("length"):
        length = callback_data.replace("length_", "")

        # Сохраняем длину в БД
        try:
            with get_db_cursor() as cur:
                cur.execute(
                    """
                    UPDATE user_states 
                    SET length = %s
                    WHERE user_id = %s
                    """,
                    (length, user_id),
                )
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения длины для {user_id}: {e}")
            await query.edit_message_text("❌ Ошибка. Попробуйте снова /add")
            return

        await query.edit_message_text(
            "• Выберите тип расцветки:",
            reply_markup=color_types_keyboard(),
        )
        return

    # Обработка выбора типа расцветки
    if callback_data.startswith("colortype_") and not user_state.get("color_type"):
        color_type = callback_data.replace("colortype_", "")

        # Сохраняем тип расцветки в БД
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
            logger.error(f"❌ Ошибка сохранения типа расцветки для {user_id}: {e}")
            await query.edit_message_text("❌ Ошибка. Попробуйте снова /add")
            return

        await query.edit_message_text(
            "• Выберите расцветку:",
            reply_markup=colors_keyboard(color_type),
        )
        return

    # Обработка выбора расцветки
    if callback_data.startswith("color_") and not user_state.get("color"):
        color = callback_data.replace("color_", "")

        # Сохраняем расцветку в БД
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
            logger.error(f"❌ Ошибка сохранения расцветки для {user_id}: {e}")
            await query.edit_message_text("❌ Ошибка. Попробуйте снова /add")
            return

        await query.edit_message_text(
            "💳 Выберите способ оплаты:",
            reply_markup=payment_methods_keyboard(),
        )
        return

    # Обработка ручного ввода цены
    if callback_data == "manual_price" and not user_state.get("payment_method"):
        # Сохраняем флаг ручного ввода в контексте
        context.user_data["manual_price_input"] = True

        # Получаем все данные пользователя
        try:
            with get_db_cursor() as cur:
                cur.execute(
                    "SELECT * FROM user_states WHERE user_id = %s",
                    (user_id,),
                )
                user_data = cur.fetchone()
        except Exception as e:
            logger.error(f"❌ Ошибка получения данных пользователя {user_id}: {e}")
            await query.edit_message_text("❌ Ошибка. Попробуйте снова /add")
            return

        if not user_data:
            await query.edit_message_text("❌ Данные не найдены. Попробуйте снова /add")
            return

        # Ищем цену в каталоге для отображения
        price = get_product_price_from_catalog(
            user_data["product_type"],
            user_data["width"],
            user_data["size"],
            user_data["length"],
            user_data["color_type"],
            user_data["color"],
        )

        # Сохраняем данные в контексте
        context.user_data["user_data"] = user_data
        context.user_data["auto_price"] = price  # Сохраняем автоматическую цену

        await query.edit_message_text(
            f"• Автоматическая цена: {price:,.2f} руб.\n\n"
            f"• Введите новую цену вручную (число, например: 1500.50):"
        )
        return

    # Обработка выбора способа оплаты
    if callback_data.startswith("payment_") and not user_state.get("payment_method"):
        payment_method = callback_data.replace("payment_", "")

        # Сохраняем способ оплаты в БД
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
            logger.error(f"❌ Ошибка сохранения способа оплаты для {user_id}: {e}")
            await query.edit_message_text("❌ Ошибка. Попробуйте снова /add")
            return

        # Получаем все данные пользователя
        try:
            with get_db_cursor() as cur:
                cur.execute(
                    "SELECT * FROM user_states WHERE user_id = %s",
                    (user_id,),
                )
                user_data = cur.fetchone()
        except Exception as e:
            logger.error(f"❌ Ошибка получения данных пользователя {user_id}: {e}")
            await query.edit_message_text("❌ Ошибка. Попробуйте снова /add")
            return

        if not user_data:
            await query.edit_message_text("❌ Данные не найдены. Попробуйте снова /add")
            return

        # Ищем цену в каталоге
        price = get_product_price_from_catalog(
            user_data["product_type"],
            user_data["width"],
            user_data["size"],
            user_data["length"],
            user_data["color_type"],
            user_data["color"],
        )

        # Запрашиваем количество
        context.user_data["price"] = price
        context.user_data["user_data"] = user_data

        await query.edit_message_text(f"• Введите количество товаров (целое число):")
        return


# ==================== ОБРАБОТЧИКИ СООБЩЕНИЙ ====================
async def handle_quantity(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик ввода количества товаров"""
    if context.user_data.get("manual_price_input"):
        return

    user_id = update.message.from_user.id

    try:
        quantity = int(update.message.text.strip())
        if quantity <= 0:
            await update.message.reply_text(
                "❌ Количество должно быть положительным числом. Попробуйте снова:"
            )
            return
    except ValueError:
        await update.message.reply_text(
            "❌ Пожалуйста, введите целое число. Попробуйте снова:"
        )
        return

    # Получаем данные из контекста
    price = context.user_data.get("manual_price") or context.user_data.get("price", 0)
    user_data = context.user_data.get("user_data", {})

    # Вычисляем общую сумму
    total_amount = price * quantity

    # Формируем данные для записи
    record_data = [
        user_data["channel"],  # Канал продаж
        user_data["product_type"],  # Тип товара
        user_data["width"] or "",  # Ширина
        user_data["size"] or "",  # Размер
        user_data["length"] or "",  # Длина
        user_data["color_type"] or "",  # Тип расцветки
        user_data["color"],  # Расцветка
        quantity,  # Количество
        price,  # Цена
        total_amount,  # Общая сумма
        user_data["payment_method"],  # Способ оплаты
        datetime.now().strftime("%d.%m.%Y"),  # Дата
    ]

    # Записываем в Google Таблицу
    try:
        sheet = get_google_sheet_cached()
        sheet.append_row(record_data)
        logger.info(f"✅ Запись добавлена в Google Таблицу: {record_data}")
    except Exception as e:
        logger.error(f"❌ Ошибка записи в Google Таблицу: {e}")
        await update.message.reply_text("❌ Ошибка записи данных. Попробуйте снова.")
        return

    # Очищаем состояние пользователя в БД
    try:
        with get_db_cursor() as cur:
            cur.execute(
                "DELETE FROM user_states WHERE user_id = %s",
                (user_id,),
            )
    except Exception as e:
        logger.error(f"❌ Ошибка очистки состояния пользователя {user_id}: {e}")

    # Формируем сообщение с итогами
    summary_message = (
        f"✅ *Продажа добавлена!*\n\n"
        f"• Канал: {user_data['channel']}\n"
        f"• Товар: {user_data['product_type']}\n"
    )

    if user_data["width"]:
        summary_message += f"• Ширина: {user_data['width']}\n"
    if user_data["size"]:
        summary_message += f"• Размер: {user_data['size']}\n"
    if user_data["length"]:
        summary_message += f"• Длина: {user_data['length']}\n"
    if user_data["color_type"]:
        summary_message += f"• Тип расцветки: {user_data['color_type']}\n"

    summary_message += (
        f"• Расцветка: {user_data['color']}\n"
        f"• Количество: {quantity} шт.\n"
        f"• Цена: {price:,.2f} руб.\n"
        f"• Сумма: {total_amount:,.2f} руб.\n"
        f"• Оплата: {user_data['payment_method']}\n"
        f"• Дата: {datetime.now().strftime('%d.%m.%Y')}"
    )

    await update.message.reply_text(summary_message, parse_mode="Markdown")


async def handle_manual_price(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик ручного ввода цены"""
    user_id = update.message.from_user.id

    try:
        manual_price = float(update.message.text.strip().replace(",", "."))
        if manual_price <= 0:
            await update.message.reply_text(
                "❌ Цена должна быть положительным числом. Попробуйте снова:"
            )
            return
    except ValueError:
        await update.message.reply_text(
            "❌ Пожалуйста, введите число (например: 1500.50). Попробуйте снова:"
        )
        return

    # Сохраняем ручную цену в контексте
    context.user_data["manual_price"] = manual_price
    context.user_data["manual_price_input"] = False  # Сбрасываем флаг

    # Запрашиваем способ оплаты
    await update.message.reply_text(
        f"• Новая цена: {manual_price:,.2f} руб.\n\n" f"💳 Выберите способ оплаты:",
        reply_markup=payment_methods_keyboard(),
    )


async def handle_message_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Роутер для обработки текстовых сообщений"""
    if context.user_data.get("manual_price_input"):
        await handle_manual_price(update, context)
    else:
        await handle_quantity(update, context)

async def handle_expense_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик ввода суммы расхода"""
    user_id = update.message.from_user.id

    try:
        amount = float(update.message.text.strip().replace(",", "."))
        if amount <= 0:
            await update.message.reply_text(
                "❌ Сумма должна быть положительным числом. Попробуйте снова:"
            )
            return
    except ValueError:
        await update.message.reply_text(
            "❌ Пожалуйста, введите число (например: 1500.50). Попробуйте снова:"
        )
        return

    # Сохраняем сумму в контексте
    if 'expense_data' not in context.user_data:
        context.user_data['expense_data'] = {}
    
    context.user_data['expense_data']['amount'] = amount
    
    await update.message.reply_text(
        f"💵 Сумма: {amount:,.2f} руб.\n\n"
        f"📝 Введите комментарий к расходу (или нажмите /skip чтобы пропустить):"
    )

async def handle_expense_comment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик ввода комментария к расходу"""
    user_id = update.message.from_user.id
    comment = update.message.text.strip()

    # Сохраняем комментарий
    context.user_data['expense_data']['comment'] = comment
    
    # Записываем расход в таблицу
    await save_expense_to_sheet(update, context)

async def save_expense_to_sheet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Сохраняет расход в Google Таблицу"""
    user_id = update.message.from_user.id
    expense_data = context.user_data.get('expense_data', {})
    
    if not expense_data:
        await update.message.reply_text("❌ Ошибка: данные расхода не найдены")
        return

    # Формируем данные для записи
    record_data = [
        expense_data.get('category', ''),  # Категория расходов
        expense_data.get('amount', 0),     # Сумма
        datetime.now().strftime("%d.%m.%Y"),  # Дата
        expense_data.get('comment', '')    # Комментарий
    ]

    # Записываем в Google Таблицу
    try:
        sheet = get_google_sheet_cached()
        expenses_sheet = sheet.spreadsheet.worksheet(EXPENSES_SHEET_NAME)
        expenses_sheet.append_row(record_data)
        
        logger.info(f"✅ Расход добавлен в Google Таблицу: {record_data}")
        
        # Формируем сообщение об успехе
        success_message = (
            f"✅ *Расход добавлен!*\n\n"
            f"• Категория: {expense_data['category']}\n"
            f"• Сумма: {expense_data['amount']:,.2f} руб.\n"
            f"• Дата: {datetime.now().strftime('%d.%m.%Y')}\n"
        )
        
        if expense_data.get('comment'):
            success_message += f"• Комментарий: {expense_data['comment']}"
        
        await update.message.reply_text(success_message, parse_mode="Markdown")
        
        # Очищаем данные
        del context.user_data['expense_data']
        
    except Exception as e:
        logger.error(f"❌ Ошибка записи расхода в Google Таблицу: {e}")
        await update.message.reply_text("❌ Ошибка записи данных. Попробуйте снова.")
        
async def skip_expense_comment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик пропуска комментария к расходу"""
    # Устанавливаем пустой комментарий
    if 'expense_data' not in context.user_data:
        context.user_data['expense_data'] = {}
    
    context.user_data['expense_data']['comment'] = ''
    
    # Записываем расход
    await save_expense_to_sheet(update, context)

async def handle_message_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Роутер для обработки текстовых сообщений"""
    if context.user_data.get("manual_price_input"):
        await handle_manual_price(update, context)
    elif 'expense_data' in context.user_data:
        # Определяем этап ввода расхода
        expense_data = context.user_data.get('expense_data', {})
        
        if 'amount' not in expense_data:
            await handle_expense_amount(update, context)
        else:
            await handle_expense_comment(update, context)
    else:
        await handle_quantity(update, context)    

# ==================== ОБРАБОТЧИК КОМАНДЫ ДЛЯ ОЧИСТКИ КЭША ====================


async def clear_cache(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /clearcache для очистки кэша"""
    try:
        # Очищаем все кэшированные функции
        get_google_sheet_cached.cache_clear()
        get_channels_from_sheet.cache_clear()
        get_payment_methods_from_sheet.cache_clear()
        get_reference_data.cache_clear()

        logger.info("🧹 Кэш успешно очищен")
        await update.message.reply_text("✅ Кэш успешно очищен!")

    except Exception as e:
        logger.error(f"❌ Ошибка очистки кэша: {e}")
        await update.message.reply_text("❌ Ошибка при очистке кэша")


# ==================== ОСНОВНАЯ ФУНКЦИЯ ====================
def main():
    """Основная функция запуска бота"""
    logger.info("🚀 Запуск бота...")

    # Инициализация БД
    init_db()

    # Создаем приложение
    application = Application.builder().token(BOT_TOKEN).build()

    # Добавляем обработчики команд
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("add", add_entry))
    application.add_handler(CommandHandler("addexpense", add_expense))
    application.add_handler(CommandHandler("report", generate_report))
    application.add_handler(CommandHandler("clearcache", clear_cache))
    application.add_handler(CommandHandler("skip", skip_expense_comment))

    # Добавляем обработчики callback запросов
    application.add_handler(CallbackQueryHandler(handle_callback_query))

    # Добавляем обработчик сообщений (для ввода количества)
    application.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message_input)
    )

    # Запускаем бота
    application.run_polling(allowed_updates=Update.ALL_TYPES)
    logger.info("🤖 Бот запущен и готов к работе!")


if __name__ == "__main__":
    main()
