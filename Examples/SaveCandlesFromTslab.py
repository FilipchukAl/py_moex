import sys
import os
from datetime import datetime, timedelta
# Добавляем корневую директорию в sys.path
root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(root_dir)
# Импортируем функции из py_moex.py
import py_moex

#------------------------------------------------------------------------------------------
# Для работы функции необходимо указать ticker, start_date, end_date, interval
# Файл с данными сохранится в корневой директории (py_moex)

# Тикер актива SZH5
ticker = "SZH5"

# Указание конкретных дат 
# start_date = datetime(2024, 12, 18) # Начальная дата: 18 декабря 2024 года 
# end_date = datetime(2024, 12, 25) # Конечная дата: 25 декабря 2024 года
# Указание "плавающей" даты например последние 60 дней
start_date = datetime.now() - timedelta(days=60)
end_date = datetime.now()

# Выбираем интервал свечей, например минутные свечи
interval = 1 
#------------------------------------------------------------------------------------------

# Вызываем функцию получения свечей
candles = py_moex.get_candles(ticker, start_date, end_date, interval=1)
print(candles)
# Сохраняем данные в корневую директорию
# Форматирование дат для использования в имени файла
start_date_str = start_date.strftime("%Y-%m-%d")
end_date_str = end_date.strftime("%Y-%m-%d")
# Формируем путь и имя сохраняемого файла
file_path = os.path.join(root_dir, f"{ticker}_{start_date_str}_{end_date_str}_interval_{interval}.txt")
# Сохраняем файл
py_moex.get_candles_save(candles, ticker, file_path)