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

        # Группируем по типам товаров
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
            [InlineKeyboardButton("❌ Отмена", callback_data="cancel")],
        ]
        return InlineKeyboardMarkup(keyboard)


def confirm_keyboard():
    """Клавиатура подтверждения"""
    keyboard = [
        [
            InlineKeyboardButton("✅ Да", callback_data="confirm_yes"),
            InlineKeyboardButton("❌ Нет", callback_data="confirm_no"),
        ]
    ]
    return InlineKeyboardMarkup(keyboard)


# ==================== ОБРАБОТЧИКИ ====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start"""
    user = update.effective_user
    logger.info(f"👋 Пользователь {user.first_name} ({user.id}) запустил бота")

    welcome_text = (
        f"Привет, {user.first_name}! 👋\n\n"
        "Я бот для учета продаж. Вот что я умею:\n\n"
        "📝 *Добавить продажу* - начать оформление новой продажи\n"
        "📊 *Отчет по каналам* - посмотреть статистику по каналам продаж\n"
        "📦 *Отчет по товарам* - посмотреть статистику по типам товаров\n"
        "🔄 *Обновить данные* - обновить кеш данных из Google Таблиц\n"
        "❓ *Помощь* - показать эту справку\n\n"
        "Выберите действие:"
    )

    keyboard = [
        [
            InlineKeyboardButton("📝 Добавить продажу", callback_data="add_sale"),
            InlineKeyboardButton("📊 Отчет по каналам", callback_data="channel_report"),
        ],
        [
            InlineKeyboardButton("📦 Отчет по товарам", callback_data="product_report"),
            InlineKeyboardButton("🔄 Обновить данные", callback_data="refresh_data"),
        ],
        [InlineKeyboardButton("❓ Помощь", callback_data="help")],
    ]

    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        welcome_text, reply_markup=reply_markup, parse_mode="Markdown"
    )


async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик нажатий на кнопки"""
    query = update.callback_query
    await query.answer()

    user_id = query.from_user.id
    data = query.data

    logger.info(f"🖱️ Пользователь {user_id} нажал кнопку: {data}")

    # Обработка основных команд
    if data == "add_sale":
        await start_sale_process(update, context)
    elif data == "channel_report":
        await show_channel_report(update, context)
    elif data == "product_report":
        await show_product_report(update, context)
    elif data == "refresh_data":
        await refresh_data(update, context)
    elif data == "help":
        await show_help(update, context)
    elif data == "cancel":
        await cancel_operation(update, context)

    # Обработка шагов оформления продажи
    elif data.startswith("channel_"):
        await handle_channel_selection(update, context, data)
    elif data.startswith("type_"):
        await handle_product_type_selection(update, context, data)
    elif data.startswith("width_"):
        await handle_width_selection(update, context, data)
    elif data.startswith("size_"):
        await handle_size_selection(update, context, data)
    elif data.startswith("length_"):
        await handle_length_selection(update, context, data)
    elif data.startswith("colortype_"):
        await handle_color_type_selection(update, context, data)
    elif data.startswith("color_"):
        await handle_color_selection(update, context, data)
    elif data.startswith("payment_"):
        await handle_payment_selection(update, context, data)
    elif data.startswith("confirm_"):
        await handle_confirmation(update, context, data)


async def start_sale_process(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начинает процесс оформления продажи"""
    try:
        query = update.callback_query
        user_id = query.from_user.id

        # Очищаем состояние пользователя
        with get_db_cursor() as cur:
            cur.execute(
                "DELETE FROM user_states WHERE user_id = %s",
                (user_id,),
            )

        # Предлагаем выбрать канал продаж
        keyboard = sales_channels_keyboard()
        await query.edit_message_text(
            "📊 *Выберите канал продаж:*",
            reply_markup=keyboard,
            parse_mode="Markdown",
        )

    except Exception as e:
        logger.error(f"❌ Ошибка начала процесса продажи: {e}")
        await handle_error(update, context, e)


async def handle_channel_selection(
    update: Update, context: ContextTypes.DEFAULT_TYPE, data: str
):
    """Обрабатывает выбор канала продаж"""
    try:
        query = update.callback_query
        user_id = query.from_user.id
        channel = data.replace("channel_", "")

        # Сохраняем в БД
        with get_db_cursor() as cur:
            cur.execute(
                """
                INSERT INTO user_states (user_id, channel)
                VALUES (%s, %s)
                ON CONFLICT (user_id) DO UPDATE SET channel = EXCLUDED.channel
                """,
                (user_id, channel),
            )

        # Предлагаем выбрать тип товара
        keyboard = product_types_keyboard()
        await query.edit_message_text(
            "🏷️ *Выберите тип товара:*",
            reply_markup=keyboard,
            parse_mode="Markdown",
        )

    except Exception as e:
        logger.error(f"❌ Ошибка обработки выбора канала: {e}")
        await handle_error(update, context, e)


async def handle_product_type_selection(
    update: Update, context: ContextTypes.DEFAULT_TYPE, data: str
):
    """Обрабатывает выбор типа товара"""
    try:
        query = update.callback_query
        user_id = query.from_user.id
        product_type = data.replace("type_", "")

        # Получаем информацию о типе товара
        ref_data = get_reference_data()
        product_info = next(
            (p for p in ref_data["product_types"] if p["type"] == product_type), None
        )

        if not product_info:
            await query.edit_message_text(
                "❌ Ошибка: тип товара не найден в справочнике"
            )
            return

        # Сохраняем в БД
        with get_db_cursor() as cur:
            cur.execute(
                """
                UPDATE user_states 
                SET product_type = %s
                WHERE user_id = %s
                """,
                (product_type, user_id),
            )

        # В зависимости от типа товара предлагаем следующие шаги
        if product_info["has_width"]:
            keyboard = widths_keyboard()
            await query.edit_message_text(
                "📏 *Выберите ширину стропы:*",
                reply_markup=keyboard,
                parse_mode="Markdown",
            )
        else:
            # Для товаров без ширины переходим к выбору типа расцветки
            keyboard = color_types_keyboard()
            await query.edit_message_text(
                "🎨 *Выберите тип расцветки:*",
                reply_markup=keyboard,
                parse_mode="Markdown",
            )

    except Exception as e:
        logger.error(f"❌ Ошибка обработки выбора типа товара: {e}")
        await handle_error(update, context, e)


async def handle_width_selection(
    update: Update, context: ContextTypes.DEFAULT_TYPE, data: str
):
    """Обрабатывает выбор ширины стропы"""
    try:
        query = update.callback_query
        user_id = query.from_user.id
        width = data.replace("width_", "")

        # Сохраняем в БД
        with get_db_cursor() as cur:
            cur.execute(
                """
                UPDATE user_states 
                SET width = %s
                WHERE user_id = %s
                """,
                (width, user_id),
            )

        # Получаем информацию о типе товара
        with get_db_cursor() as cur:
            cur.execute(
                "SELECT product_type FROM user_states WHERE user_id = %s", (user_id,)
            )
            result = cur.fetchone()
            product_type = result["product_type"] if result else None

        ref_data = get_reference_data()
        product_info = next(
            (p for p in ref_data["product_types"] if p["type"] == product_type), None
        )

        if not product_info:
            await query.edit_message_text("❌ Ошибка: тип товара не найден")
            return

        # В зависимости от типа товара предлагаем следующие шаги
        if product_info["has_size"]:
            keyboard = sizes_keyboard(width)
            await query.edit_message_text(
                "📐 *Выберите размер:*",
                reply_markup=keyboard,
                parse_mode="Markdown",
            )
        elif product_info["has_length"]:
            keyboard = lengths_keyboard(width)
            await query.edit_message_text(
                "📏 *Выберите длину:*",
                reply_markup=keyboard,
                parse_mode="Markdown",
            )
        else:
            # Переходим к выбору типа расцветки
            keyboard = color_types_keyboard()
            await query.edit_message_text(
                "🎨 *Выберите тип расцветки:*",
                reply_markup=keyboard,
                parse_mode="Markdown",
            )

    except Exception as e:
        logger.error(f"❌ Ошибка обработки выбора ширины: {e}")
        await handle_error(update, context, e)


async def handle_size_selection(
    update: Update, context: ContextTypes.DEFAULT_TYPE, data: str
):
    """Обрабатывает выбор размера"""
    try:
        query = update.callback_query
        user_id = query.from_user.id
        size = data.replace("size_", "")

        # Сохраняем в БД
        with get_db_cursor() as cur:
            cur.execute(
                """
                UPDATE user_states 
                SET size = %s
                WHERE user_id = %s
                """,
                (size, user_id),
            )

        # Переходим к выбору типа расцветки
        keyboard = color_types_keyboard()
        await query.edit_message_text(
            "🎨 *Выберите тип расцветки:*",
            reply_markup=keyboard,
            parse_mode="Markdown",
        )

    except Exception as e:
        logger.error(f"❌ Ошибка обработки выбора размера: {e}")
        await handle_error(update, context, e)


async def handle_length_selection(
    update: Update, context: ContextTypes.DEFAULT_TYPE, data: str
):
    """Обрабатывает выбор длины"""
    try:
        query = update.callback_query
        user_id = query.from_user.id
        length = data.replace("length_", "")

        # Сохраняем в БД
        with get_db_cursor() as cur:
            cur.execute(
                """
                UPDATE user_states 
                SET length = %s
                WHERE user_id = %s
                """,
                (length, user_id),
            )

        # Переходим к выбору типа расцветки
        keyboard = color_types_keyboard()
        await query.edit_message_text(
            "🎨 *Выберите тип расцветки:*",
            reply_markup=keyboard,
            parse_mode="Markdown",
        )

    except Exception as e:
        logger.error(f"❌ Ошибка обработки выбора длины: {e}")
        await handle_error(update, context, e)


async def handle_color_type_selection(
    update: Update, context: ContextTypes.DEFAULT_TYPE, data: str
):
    """Обрабатывает выбор типа расцветки"""
    try:
        query = update.callback_query
        user_id = query.from_user.id
        color_type = data.replace("colortype_", "")

        # Сохраняем в БД
        with get_db_cursor() as cur:
            cur.execute(
                """
                UPDATE user_states 
                SET color_type = %s
                WHERE user_id = %s
                """,
                (color_type, user_id),
            )

        # Получаем информацию о типе товара
        with get_db_cursor() as cur:
            cur.execute(
                "SELECT product_type FROM user_states WHERE user_id = %s", (user_id,)
            )
            result = cur.fetchone()
            product_type = result["product_type"] if result else None

        # Для Лежанки и Бусы показываем все расцветки
        if product_type in ["Лежанка", "Бусы"]:
            keyboard = all_colors_keyboard()
            await query.edit_message_text(
                "🌈 *Выберите расцветку:*",
                reply_markup=keyboard,
                parse_mode="Markdown",
            )
        else:
            keyboard = colors_keyboard(color_type)
            await query.edit_message_text(
                "🌈 *Выберите расцветку:*",
                reply_markup=keyboard,
                parse_mode="Markdown",
            )

    except Exception as e:
        logger.error(f"❌ Ошибка обработки выбора типа расцветки: {e}")
        await handle_error(update, context, e)


async def handle_color_selection(
    update: Update, context: ContextTypes.DEFAULT_TYPE, data: str
):
    """Обрабатывает выбор расцветки"""
    try:
        query = update.callback_query
        user_id = query.from_user.id
        color = data.replace("color_", "")

        # Сохраняем в БД
        with get_db_cursor() as cur:
            cur.execute(
                """
                UPDATE user_states 
                SET color = %s
                WHERE user_id = %s
                """,
                (color, user_id),
            )

        # Переходим к выбору способа оплаты
        keyboard = payment_methods_keyboard()
        await query.edit_message_text(
            "💳 *Выберите способ оплаты:*",
            reply_markup=keyboard,
            parse_mode="Markdown",
        )

    except Exception as e:
        logger.error(f"❌ Ошибка обработки выбора расцветки: {e}")
        await handle_error(update, context, e)


async def handle_payment_selection(
    update: Update, context: ContextTypes.DEFAULT_TYPE, data: str
):
    """Обрабатывает выбор способа оплаты"""
    try:
        query = update.callback_query
        user_id = query.from_user.id
        payment_method = data.replace("payment_", "")

        # Сохраняем в БД
        with get_db_cursor() as cur:
            cur.execute(
                """
                UPDATE user_states 
                SET payment_method = %s
                WHERE user_id = %s
                """,
                (payment_method, user_id),
            )

        # Получаем все данные о продаже
        with get_db_cursor() as cur:
            cur.execute("SELECT * FROM user_states WHERE user_id = %s", (user_id,))
            sale_data = cur.fetchone()

        if not sale_data:
            await query.edit_message_text("❌ Ошибка: данные продажи не найдены")
            return

        # Находим цену товара
        price = get_product_price_from_catalog(
            sale_data["product_type"],
            sale_data["width"],
            sale_data["size"],
            sale_data["length"],
            sale_data["color_type"],
            sale_data["color"],
        )

        # Формируем текст подтверждения
        confirmation_text = (
            f"📋 *ПОДТВЕРЖДЕНИЕ ПРОДАЖИ*\n\n"
            f"📊 *Канал:* {sale_data['channel']}\n"
            f"🏷️ *Товар:* {sale_data['product_type']}\n"
        )

        if sale_data["width"]:
            confirmation_text += f"📏 *Ширина:* {sale_data['width']}\n"
        if sale_data["size"]:
            confirmation_text += f"📐 *Размер:* {sale_data['size']}\n"
        if sale_data["length"]:
            confirmation_text += f"📏 *Длина:* {sale_data['length']}\n"

        confirmation_text += (
            f"🎨 *Тип расцветки:* {sale_data['color_type']}\n"
            f"🌈 *Расцветка:* {sale_data['color']}\n"
            f"💳 *Способ оплаты:* {sale_data['payment_method']}\n"
            f"💰 *Цена:* {price:,.2f} руб.\n\n"
            f"Введите количество товаров:"
        )

        # Сохраняем цену в контексте
        context.user_data["current_price"] = price

        await query.edit_message_text(confirmation_text, parse_mode="Markdown")

        # Ждем ввода количества
        context.user_data["awaiting_quantity"] = True

    except Exception as e:
        logger.error(f"❌ Ошибка обработки выбора способа оплаты: {e}")
        await handle_error(update, context, e)


async def handle_quantity_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает ввод количества товаров"""
    try:
        user_id = update.message.from_user.id
        quantity_text = update.message.text

        # Проверяем, что введено число
        try:
            quantity = int(quantity_text)
            if quantity <= 0:
                await update.message.reply_text(
                    "❌ Количество должно быть положительным числом. Попробуйте еще раз:"
                )
                return
        except ValueError:
            await update.message.reply_text(
                "❌ Пожалуйста, введите целое число. Попробуйте еще раз:"
            )
            return

        # Получаем данные о продаже
        with get_db_cursor() as cur:
            cur.execute("SELECT * FROM user_states WHERE user_id = %s", (user_id,))
            sale_data = cur.fetchone()

        if not sale_data:
            await update.message.reply_text("❌ Ошибка: данные продажи не найдены")
            return

        # Получаем цену из контекста
        price = context.user_data.get("current_price", 0)
        total_amount = price * quantity

        # Формируем финальное подтверждение
        confirmation_text = (
            f"✅ *ФИНАЛЬНОЕ ПОДТВЕРЖДЕНИЕ*\n\n"
            f"• *Канал:* {sale_data['channel']}\n"
            f"• *Товар:* {sale_data['product_type']}\n"
        )

        if sale_data["width"]:
            confirmation_text += f"📏 *Ширина:* {sale_data['width']}\n"
        if sale_data["size"]:
            confirmation_text += f"📐 *Размер:* {sale_data['size']}\n"
        if sale_data["length"]:
            confirmation_text += f"📏 *Длина:* {sale_data['length']}\n"

        confirmation_text += (
            f"• *Тип расцветки:* {sale_data['color_type']}\n"
            f"• *Расцветка:* {sale_data['color']}\n"
            f"• *Способ оплаты:* {sale_data['payment_method']}\n"
            f"• *Цена за шт.:* {price:,.2f} руб.\n"
            f"• *Количество:* {quantity} шт.\n"
            f"• *Общая сумма:* {total_amount:,.2f} руб.\n\n"
            f"• *Подтверждаете продажу?*"
        )

        # Сохраняем количество и общую сумму в контексте
        context.user_data["quantity"] = quantity
        context.user_data["total_amount"] = total_amount

        keyboard = confirm_keyboard()
        await update.message.reply_text(
            confirmation_text, reply_markup=keyboard, parse_mode="Markdown"
        )

        # Сбрасываем флаг ожидания количества
        context.user_data["awaiting_quantity"] = False

    except Exception as e:
        logger.error(f"❌ Ошибка обработки ввода количества: {e}")
        await handle_error(update, context, e)


async def handle_confirmation(
    update: Update, context: ContextTypes.DEFAULT_TYPE, data: str
):
    """Обрабатывает подтверждение продажи"""
    try:
        query = update.callback_query
        user_id = query.from_user.id

        if data == "confirm_yes":
            # Получаем данные о продаже
            with get_db_cursor() as cur:
                cur.execute("SELECT * FROM user_states WHERE user_id = %s", (user_id,))
                sale_data = cur.fetchone()

            if not sale_data:
                await query.edit_message_text("❌ Ошибка: данные продажи не найдены")
                return

            # Получаем количество и сумму из контекста
            quantity = context.user_data.get("quantity", 1)
            total_amount = context.user_data.get("total_amount", 0)

            # Записываем продажу в Google Таблицу
            try:
                sheet = get_google_sheet_cached()

                # Формируем данные для записи
                current_date = datetime.now().strftime("%d.%m.%Y")
                row_data = [
                    sale_data["channel"],
                    sale_data["product_type"],
                    sale_data["width"] or "",
                    sale_data["size"] or "",
                    sale_data["length"] or "",
                    sale_data["color_type"] or "",
                    sale_data["color"] or "",
                    quantity,
                    context.user_data.get("current_price", 0),
                    total_amount,
                    sale_data["payment_method"],
                    current_date,
                ]

                # Добавляем новую строку
                sheet.append_row(row_data)

                logger.info(f"✅ Продажа записана в Google Таблицу: {row_data}")

                await query.edit_message_text(
                    f"✅ *Продажа успешно записана!*\n\n"
                    f"• Сумма: {total_amount:,.2f} руб.\n"
                    f"• Дата: {current_date}\n\n"
                    f"Что хотите сделать дальше?",
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup(
                        [
                            [
                                InlineKeyboardButton(
                                    "📝 Новая продажа", callback_data="add_sale"
                                )
                            ],
                            [
                                InlineKeyboardButton(
                                    "🏠 Главное меню", callback_data="main_menu"
                                )
                            ],
                        ]
                    ),
                )

                # Очищаем состояние пользователя
                with get_db_cursor() as cur:
                    cur.execute(
                        "DELETE FROM user_states WHERE user_id = %s", (user_id,)
                    )

                # Очищаем контекст
                context.user_data.clear()

            except Exception as e:
                logger.error(f"❌ Ошибка записи в Google Таблицу: {e}")
                await query.edit_message_text(
                    "❌ Ошибка при записи продажи. Попробуйте еще раз."
                )

        else:  # confirm_no
            await query.edit_message_text(
                "❌ *Продажа отменена.*\n\nЧто хотите сделать дальше?",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton(
                                "📝 Новая продажа", callback_data="add_sale"
                            )
                        ],
                        [
                            InlineKeyboardButton(
                                "🏠 Главное меню", callback_data="main_menu"
                            )
                        ],
                    ]
                ),
            )

    except Exception as e:
        logger.error(f"❌ Ошибка обработки подтверждения: {e}")
        await handle_error(update, context, e)


async def show_channel_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает отчет по каналам продаж"""
    try:
        query = update.callback_query
        sales_data = get_sales_data()
        report = generate_channel_report(sales_data)

        await query.edit_message_text(
            report,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]]
            ),
        )

    except Exception as e:
        logger.error(f"❌ Ошибка показа отчета по каналам: {e}")
        await handle_error(update, context, e)


async def show_product_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает отчет по типам товаров"""
    try:
        query = update.callback_query
        sales_data = get_sales_data()
        report = generate_product_report(sales_data)

        await query.edit_message_text(
            report,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]]
            ),
        )

    except Exception as e:
        logger.error(f"❌ Ошибка показа отчета по товарам: {e}")
        await handle_error(update, context, e)


async def refresh_data(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обновляет кешированные данные"""
    try:
        query = update.callback_query

        # Очищаем кеш
        get_google_sheet_cached.cache_clear()
        get_channels_from_sheet.cache_clear()
        get_payment_methods_from_sheet.cache_clear()
        get_reference_data.cache_clear()

        # Перезагружаем данные
        get_google_sheet_cached()
        get_channels_from_sheet()
        get_payment_methods_from_sheet()
        get_reference_data()

        await query.edit_message_text(
            "🔄 *Данные успешно обновлены!*\n\nКеш очищен, данные перезагружены из Google Таблиц.",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]]
            ),
        )

    except Exception as e:
        logger.error(f"❌ Ошибка обновления данных: {e}")
        await handle_error(update, context, e)


async def show_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает справку"""
    try:
        query = update.callback_query

        help_text = (
            "🤖 *ПОМОЩЬ ПО БОТУ*\n\n"
            "Я помогаю вести учет продаж. Вот что я умею:\n\n"
            "📝 *Добавить продажу* - пошаговое оформление новой продажи\n"
            "📊 *Отчет по каналам* - статистика продаж по каналам\n"
            "📦 *Отчет по товарам* - статистика по типам товаров\n"
            "🔄 *Обновить данные* - обновление данных из Google Таблиц\n\n"
            "*Процесс оформления продажи:*\n"
            "1. Выберите канал продаж\n"
            "2. Выберите тип товара\n"
            "3. Выберите параметры товара (ширина, размер, длина)\n"
            "4. Выберите расцветку\n"
            "5. Выберите способ оплаты\n"
            "6. Введите количество\n"
            "7. Подтвердите продажу\n\n"
            "Все данные автоматически записываются в Google Таблицу."
        )

        await query.edit_message_text(
            help_text,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]]
            ),
        )

    except Exception as e:
        logger.error(f"❌ Ошибка показа справки: {e}")
        await handle_error(update, context, e)


async def cancel_operation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отменяет текущую операцию"""
    try:
        query = update.callback_query
        user_id = query.from_user.id

        # Очищаем состояние пользователя
        with get_db_cursor() as cur:
            cur.execute("DELETE FROM user_states WHERE user_id = %s", (user_id,))

        # Очищаем контекст
        context.user_data.clear()

        await query.edit_message_text(
            "❌ *Операция отменена.*\n\nЧто хотите сделать дальше?",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "📝 Новая продажа", callback_data="add_sale"
                        )
                    ],
                    [
                        InlineKeyboardButton(
                            "🏠 Главное меню", callback_data="main_menu"
                        )
                    ],
                ]
            ),
        )

    except Exception as e:
        logger.error(f"❌ Ошибка отмены операции: {e}")
        await handle_error(update, context, e)


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает текстовые сообщения"""
    try:
        user_id = update.message.from_user.id

        # Проверяем, ожидаем ли мы ввод количества
        if context.user_data.get("awaiting_quantity", False):
            await handle_quantity_input(update, context)
            return

        # Если это не ввод количества, предлагаем начать заново
        keyboard = InlineKeyboardMarkup(
            [[InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]]
        )

        await update.message.reply_text(
            "🤔 Я не понял ваше сообщение. Выберите действие из меню:",
            reply_markup=keyboard,
        )

    except Exception as e:
        logger.error(f"❌ Ошибка обработки сообщения: {e}")
        await handle_error(update, context, e)


async def handle_error(
    update: Update, context: ContextTypes.DEFAULT_TYPE, error: Exception
):
    """Обрабатывает ошибки"""
    logger.error(f"❌ Ошибка: {error}", exc_info=True)

    try:
        if update and update.effective_message:
            await update.effective_message.reply_text(
                "❌ Произошла ошибка. Попробуйте еще раз или обратитесь к администратору.",
                reply_markup=InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton(
                                "🏠 Главное меню", callback_data="main_menu"
                            )
                        ]
                    ]
                ),
            )
    except Exception as e:
        logger.error(f"❌ Ошибка при отправке сообщения об ошибке: {e}")


# ==================== ЗАПУСК БОТА ====================
def main():
    """Основная функция запуска бота"""
    try:
        logger.info("🚀 Запуск бота...")

        # Инициализируем БД
        init_db()

        # Создаем приложение
        application = Application.builder().token(BOT_TOKEN).build()

        # Добавляем обработчики
        application.add_handler(CommandHandler("start", start))
        application.add_handler(CallbackQueryHandler(button_handler))
        application.add_handler(
            MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message)
        )

        # Обработчик ошибок
        application.add_error_handler(
            lambda update, context: handle_error(update, context, context.error)
        )

        logger.info("✅ Бот запущен и готов к работе")

        # Запускаем бота
        application.run_polling(allowed_updates=Update.ALL_TYPES)

    except Exception as e:
        logger.error(f"❌ Критическая ошибка при запуске бота: {e}")
        raise


if __name__ == "__main__":
    main()
