import requests
import os
import pandas as pd
from datetime import datetime, timedelta

def get_security_info(ticker):
    """ 
    Функция получения спецификации инструмента с MOEX. 

        Параметры: 
            ticker (str): Тикер (пример: "LKOH"). 

        Возвращает: 
            dict: Словарь с данными о ценной бумаге:
                - secid (str): Код ценной бумаги ("LKOH").
                - shortname (str): Краткое наименование ("ЛУКОЙЛ").
                - group (str): Код типа инструмента ("stock_shares").
                - market (str): Название рынка, на котором торгуется инструмент ("shares").
                - engine (str): Название торговой системы ("stock").
    """
    # Проверяем, что тикер передан как строка
    if not isinstance(ticker, str):
        raise ValueError("ticker должен быть строкой")

    # Формируем URL для запроса
    url = f"https://iss.moex.com/iss/securities/{ticker}.json"

    try:
        # Выполняем запрос
        response = requests.get(url)
        response.raise_for_status()  # Проверяем, что запрос выполнен успешно
    except requests.RequestException as e:
        raise RuntimeError(f"Ошибка при выполнении запроса: {e}")

    # Парсим JSON-ответ
    try:
        data = response.json()
    except ValueError:
        raise ValueError("Ошибка парсинга JSON")

    # Проверяем наличие разделов в ответе
    if "description" not in data or "boards" not in data:
        raise ValueError("Недопустимый формат ответа JSON")

    # Извлекаем данные из первой таблицы (description)
    description_data = {item[0]: item[2] for item in data["description"]["data"]}
    secid = description_data.get("SECID")
    shortname = description_data.get("SHORTNAME")
    group = description_data.get("GROUP")

    if not all([secid, shortname, group]):
        raise ValueError("Отсутствуют необходимые данные в разделе description")

    # Извлекаем данные из второй таблицы (boards)
    boards_data = data["boards"]["data"]
    if not boards_data or len(boards_data[0]) < 8:
        raise ValueError("Неверный формат данных в boards")

    first_row = boards_data[0]
    market = first_row[5]  # "market" находится в 6-м столбце
    engine = first_row[7]  # "engine" находится в 8-м столбце

    return {
        "secid": secid,
        "shortname": shortname,
        "group": group,
        "market": market,
        "engine": engine
    }

def get_candles(ticker, start_date, end_date, interval=1):
    """ 
    Функция получения свечей с MOEX для заданного инструмента. 

        Параметры: 
            ticker (str): Тикер (например, "LKOH" для акций или "LKH5" для фьючерсов). 
            start_date (datetime): Начальная дата. 
            end_date (datetime): Конечная дата. 
            interval (int): Интервал (по умолчанию минутные свечи). 

        Возвращает: 
            pd.DataFrame: DataFrame, содержащий полученные данные. 
    """
    # Проверка типов аргументов
    if not isinstance(ticker, str):
        raise TypeError("Параметр 'ticker' должен быть строкой (str).")
    if not isinstance(start_date, datetime):
        raise TypeError("Параметр 'start_date' должен быть объектом datetime.")
    if not isinstance(end_date, datetime):
        raise TypeError("Параметр 'end_date' должен быть объектом datetime.")

    # Получение информации о тикере
    ticker_info = get_security_info(ticker)
    if not ticker_info:
        raise ValueError(f"Информация о тикере {ticker} не найдена.")

    market = ticker_info["market"]
    engine = ticker_info["engine"]

    base_url = f"https://iss.moex.com/iss/engines/{engine}/markets/{market}/securities/{ticker}/candles.json"
    
    date_format = "%Y-%m-%d %H:%M:%S"
    current_start = start_date
    all_data = []

    while current_start <= end_date:
        for hour in range(10, 24):
            start_time = current_start.replace(hour=hour, minute=0, second=0)
            end_time = start_time + timedelta(hours=1) - timedelta(seconds=1)

            if start_time > end_date:
                break

            params = {
                "from": start_time.strftime(date_format),
                "till": end_time.strftime(date_format),
                "interval": interval
            }

            try:
                response = requests.get(base_url, params=params, timeout=2)  # Таймаут в 2 секунды
                response.raise_for_status()
                data = response.json().get("candles", {}).get("data", [])

                for candle in data:
                    all_data.append([
                        ticker,
                        interval,
                        start_time.strftime("%y%m%d"),
                        candle[6].split()[1].replace(":", ""),
                        candle[0], candle[2], candle[3], candle[1], candle[5]
                    ])
            except requests.exceptions.RequestException as e:
                print(f"Ошибка при получении данных с {start_time} по {end_time}: {e}")

        current_start += timedelta(days=1)

    return pd.DataFrame(all_data, columns=["<TICKER>", "<PER>", "<DATE>", "<TIME>", "<OPEN>", "<HIGH>", "<LOW>", "<CLOSE>", "<VOL>"])

def get_candles_save(candles, ticker, file_path=None):
    """ 
    Функция сохранения DataFrame в текстовый файл в требуемом формате.

    Параметры: 
        candles (pd.DataFrame): DataFrame, содержащий данные свечей.
        ticker (str): Тикер актива. 
        file_path (str, optional): Путь для сохранения файла. Если не указан, файл будет сохранен в директории скрипта с именем по умолчанию. 
    """
    # Установим имя файла по умолчанию, если путь не указан
    if file_path is None:
        file_path = os.path.join(os.getcwd(), f"{ticker}.csv")  # Расширение изменено на .csv

    # Проверяем, является ли `candles` DataFrame
    if not isinstance(candles, pd.DataFrame):
        print(f"Ошибка: ожидался DataFrame, но получен {type(candles).__name__}.")
        return

    try:
        # Сохраняем DataFrame в текстовом формате с разделителем ","
        candles.to_csv(file_path, index=False, header=True, sep=",", encoding="utf-8")
        print(f"Данные успешно сохранены в файл: {file_path}")
    except Exception as e:
        print(f"Ошибка при сохранении файла: {e}")

def get_last_history_candle(ticker):
    """ 
    Получение свечи последней торговой сессии. 

        Параметры: 
            ticker (str): Тикер (например, "LKOH" для акций или "LKH5" для фьючерсов).  

        Возвращает:
            dict: Словарь со следующими ключами:
                - open (float): Цена открытия  
                - close (float): Цена закрытия
                - high (float): Максимальная цена
                - low (float): Минимальная цена
                - value (float): Объем в рублях (value = volume * close)
                - volume (float): Объем в бумагах

            или None, если данные отсутствуют. 
    """
    if not isinstance(ticker, str):
        raise TypeError("Параметр 'ticker' должен быть строкой (str).")

    # Получаем параметры инструмента
    ticker_info = get_security_info(ticker)

    if not ticker_info:
        raise ValueError(f"Информация о тикере {ticker} не найдена.")

    market = ticker_info["market"]
    engine = ticker_info["engine"]

    # Формируем базовый URL для API MOEX
    base_url = f"https://iss.moex.com/iss/history/engines/{engine}/markets/{market}/securities/{ticker}.json"

    """
    Параметры history:

        start (str)(Default: 0): 
        sort_order (str)(Default: TRADEDATE): Направление сортировки "asc" - По возрастанию значения, "desc" - По убыванию
        from (str)(Default:): Дата, начиная с которой необходимо начать выводить данные. Формат: ГГГГ-ММ-ДД ЧЧ:ММ:СС.
        till (str)(Default: 2037-12-31): Дата, до которой выводить данные. Формат: ГГГГ-ММ-ДД ЧЧ:ММ:СС
        sort_column (str)(Default: TRADEDATE): Поле, по которому сортируется ответ.
        limit (str)(Default: 100): Количество доступных значений
        tradingsession (str): Показать данные только за необходимую сессию (только для фондового рынка)
                              0 - Утренняя
                              1 - Основная
                              2 - Вечерняя
                              3 - Итого
    """

    params = {
        "sort_order": "desc",
        "limit": 1
    }

    try:
        # Отправляем запрос
        response = requests.get(base_url, params=params, timeout=2)
        response.raise_for_status()
        data = response.json().get("history", {}).get("data", [])

        if data:
            # Последняя свеча
            # ["BOARDID", "TRADEDATE", "SHORTNAME", "SECID", "NUMTRADES", "VALUE", "OPEN", "LOW", "HIGH", "LEGALCLOSEPRICE", "WAPRICE", "CLOSE", "VOLUME", "MARKETPRICE2", "MARKETPRICE3", "ADMITTEDQUOTE", "MP2VALTRD", "MARKETPRICE3TRADESVALUE", "ADMITTEDVALUE", "WAVAL", "TRADINGSESSION", "CURRENCYID", "TRENDCLSPR"]
            # ['TQBR', '2024-12-30', 'ЛУКОЙЛ', 'LKOH', 53942, 5560649924.5, 7011, 7003, 7260, 7240.5, 7176, 7235, 774886, 7170, 7170, None, 4876615920, 4876615920, None, 0, 3, 'SUR', 3.39]
            result = {
                "open": float(data[0][6]),
                "close": float(data[0][11]),
                "high": float(data[0][8]),
                "low": float(data[0][7]),
                "value": float(data[0][5]),  # Объем в рублях
                "volume": float(data[0][12])  # Объем в бумагах
            }
        else:
            print("Данные за последнюю торговую сессию отсутствуют.")
            result = None

        return result
    except requests.exceptions.RequestException as e:
        print(f"Ошибка при запросе данных: {e}")
        return None

def get_last_candle(ticker, start_time, end_time, interval):
    """ 
    Получение последней свечи за заданный интервал. 

        Параметры: 
            ticker (str): Тикер (например, "LKOH" для акций или "LKH5" для фьючерсов).  
            start_time (datetime): Начальное время для получения данных.
            end_time (datetime): Конечное время для получения данных.
            interval (int): Интервал свечи (например, 1, 5, 10 минут).

        Возвращает:
            dict: Словарь со следующими ключами:
                - open (float): Цена открытия
                - close (float): Цена закрытия
                - high (float): Максимальная цена
                - low (float): Минимальная цена
                - value (float): Объем в рублях
                - volume (float): Объем в бумагах

            или None, если данные отсутствуют. 
    """
    if not isinstance(ticker, str):
        raise TypeError("Параметр 'ticker' должен быть строкой (str).")

    # Получаем параметры инструмента
    ticker_info = get_security_info(ticker)

    if not ticker_info:
        raise ValueError(f"Информация о тикере {ticker} не найдена.")

    market = ticker_info["market"]
    engine = ticker_info["engine"]

    # Формируем базовый URL для API MOEX
    base_url = f"https://iss.moex.com/iss/engines/{engine}/markets/{market}/securities/{ticker}/candles.json"

    params = {
        "from": start_time.strftime("%Y-%m-%d %H:%M:%S"),
        "till": end_time.strftime("%Y-%m-%d %H:%M:%S"),
        "interval": interval
    }

    try:
        # Отправляем запрос
        response = requests.get(base_url, params=params, timeout=2)
        response.raise_for_status()
        data = response.json().get("candles", {}).get("data", [])

        if data:
            # Последняя свеча в текущий день
            # ["open", "close", "high", "low", "value", "volume", "begin", "end"]
            # [6962, 6963, 6963, 6961.5, 487339.5, 70, '2024-12-27 18:18:00', '2024-12-27 18:18:59']
            # print(data[-1][6]) # Время начала последней свечи
            # print(data[-1][7]) # Время конца последней свечи
            last_candle = data[-1]
            result = {
                "open": float(last_candle[0]),
                "close": float(last_candle[1]),
                "high": float(last_candle[2]),
                "low": float(last_candle[3]),
                "value": float(last_candle[4]),
                "volume": float(last_candle[5])
            }
        else:
            print("Данные отсутствуют.")
            result = None

        return result
    except requests.exceptions.RequestException as e:
        print(f"Ошибка при запросе данных: {e}")
        return None

def get_last_price(ticker):
    """ 
    Получение цены закрытия последней свечи за текущую торговую сессию. 

        Параметры: 
            ticker (str): Тикер (например, "LKOH" для акций или "LKH5" для фьючерсов).  

        Возвращает: 
            float: Цена закрытия последней свечи или None, если данные отсутствуют. 
    """
    if not isinstance(ticker, str):
        raise TypeError("Параметр 'ticker' должен быть строкой (str).")

    # Минутный интервал свечи
    interval = 1
    # Берем последние 15 минут, чтобы получить из него последнюю минуту
    now = datetime.now()
    start_time = now - timedelta(minutes=15)
    end_time = now
    last_candle = get_last_candle(ticker, start_time, end_time, interval)

    if last_candle:
        close = last_candle["close"]
        return close 
    else:
        # Если за сегодня торгов не было, смотрим свечу за последний торговый день инструмента
        last_candle = get_last_history_candle(ticker)
        if last_candle:
            close = last_candle["close"]
            return close
        else:
            return None

def get_last_price_for_date(ticker, date):
    """
    Получение цены закрытия последней свечи за заданную дату.

        Параметры:
            ticker (str): Тикер (например, "LKOH" для акций или "LKH5" для фьючерсов).
            date (str or datetime): Дата в формате "YYYY-MM-DD" или объект datetime.

        Возвращает:
            float: Цена закрытия последней свечи или None, если данные отсутствуют.
    """
    if isinstance(date, str):
        date = datetime.strptime(date, "%Y-%m-%d")

    # Временные интервалы вечерней сессии
    evening_start = datetime(date.year, date.month, date.day, 23, 0, 0)
    evening_end = datetime(date.year, date.month, date.day, 23, 59, 59)
    last_candle = get_last_candle(ticker, evening_start, evening_end, 1)
    if last_candle:
        return last_candle["close"]

    # Временные интервалы основной сессии
    main_start = datetime(date.year, date.month, date.day, 18, 0, 0)
    main_end = datetime(date.year, date.month, date.day, 18, 59, 59)
    last_candle = get_last_candle(ticker, main_start, main_end, 1)
    if last_candle:
        return last_candle["close"]

    print("Данные отсутствуют.")
    return None

def get_list_assets(asset_type):
    """ 
    Получение списка активов. 

        Параметры: 
            asset_type (str): Тип активов: "shares", "otc", "etf", "futures". 

        Возвращает: 
            pd.DataFrame: DataFrame с колонками SECID и SHORTNAME.  
    """
    urls = {
        "shares": "https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities.json",
        "otc": "https://iss.moex.com/iss/engines/otc/markets/shares/boards/MTQR/securities.json",
        "etf": "https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQTF/securities.json",
        "futures": "https://iss.moex.com/iss/engines/futures/markets/forts/boards/RFUD/securities.json"
    }

    if asset_type not in urls:
        raise ValueError("Параметр 'asset_type' должен быть одним из: 'shares', 'otc', 'etf', 'futures'.")

    response = requests.get(urls[asset_type])
    response.raise_for_status()

    data = response.json()
    columns = data['securities']['columns']
    rows = data['securities']['data']

    df = pd.DataFrame(rows, columns=columns)
    return df[['SECID', 'SHORTNAME']]