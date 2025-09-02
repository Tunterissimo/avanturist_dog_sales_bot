import logging
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
)
import gspread
from google.oauth2.service_account import Credentials

# Настройка логирования
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# Настройки Google Sheets
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
CREDENTIALS_FILE = (
    "decoded-agency-470814-t9-b5de9124f4d9.json"  # Путь к вашему JSON-файлу с ключом
)
SPREADSHEET_ID = (
    "1kuZScoF0CNwzj-g86xATYJY-UsJeqQ0zEzY34tKvC2A"  # Замените на ID вашей таблицы
)


# Функция для авторизации и получения листа
def get_google_sheet():
    creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
    client = gspread.authorize(creds)
    # Открываем лист "Тест" в таблице
    sheet = client.open_by_key(SPREADSHEET_ID).worksheet("Тест")
    return sheet


# Обработчик команды /start
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_name = update.message.from_user.first_name
    help_text = f"""
Привет, {user_name}! Я бот для учета продаж.

Чтобы добавить запись, отправь мне данные в одну строку, разделенные запятой, в следующем порядке:
**Канал продаж, Наименование товара, Количество, Цена**

Например:
`Инстаграм, Кружка керамическая, 2, 350`
"""
    await update.message.reply_text(help_text, parse_mode="Markdown")


# Обработчик текстовых сообщений
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_message = update.message.text
    user_id = update.message.from_user.id

    # Разбиваем сообщение по запятым и убираем лишние пробелы
    data = [item.strip() for item in user_message.split(",")]

    # Проверяем, что получили ровно 4 элемента
    if len(data) != 4:
        error_text = """Неверный формат данных. Нужно указать 4 значения через запятую:
**Канал продаж, Наименование товара, Количество, Цена**

Пример:
`Инстаграм, Кружка керамическая, 2, 350`"""
        await update.message.reply_text(error_text, parse_mode="Markdown")
        return

    # Извлекаем и проверяем данные на числа (Количество и Цена)
    try:
        channel, product, quantity, price = data
        # Пробуем преобразовать к числам
        quantity = float(
            quantity.replace(",", ".")
        )  # На случай, если дробные через запятую
        price = float(price.replace(",", "."))
    except ValueError:
        await update.message.reply_text(
            "Ошибка: 'Количество' и 'Цена' должны быть числами."
        )
        return

    try:
        sheet = get_google_sheet()

        # Подготавливаем данные для вставки
        # Мы вставляем только первые 4 столбца (A, B, C, D).
        # Столбец E (Сумма) и F (Дата) заполнятся автоматически формулами.
        row_data = [channel, product, quantity, price]

        # Находим первую свободную строку (после заголовка)
        next_row = len(sheet.get_all_values()) + 1

        # Вставляем данные в строку, начиная с колонки A
        sheet.insert_row(row_data, next_row)

        success_text = f"""✅ Данные успешно добавлены в таблицу!
*Канал продаж:* {channel}
*Товар:* {product}
*Количество:* {quantity}
*Цена:* {price}
"""
        await update.message.reply_text(success_text, parse_mode="Markdown")
        logger.info(f"User {user_id} added record: {row_data}")

    except Exception as e:
        error_msg = "❌ Произошла ошибка при записи в Google Таблицу."
        await update.message.reply_text(error_msg)
        logger.error(f"Error for user {user_id}: {e}")


def main():
    # Создаем Application и передаем ему токен бота
    application = (
        Application.builder()
        .token("8489004330:AAHTsWtgf-1wwvtzTRSx3a2WQa7vWque8Lg")
        .build()
    )  # Замените на ваш токен

    # Добавляем обработчики
    application.add_handler(CommandHandler("start", start))
    application.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message)
    )

    # Запускаем бота
    application.run_polling()
    print("Бот запущен...")


if __name__ == "__main__":
    main()
