# handles the requests of eod data by accessing storage class
import trade_utilities.dates as dates
import sqlite3 as sql
import config
import kibot.kibot_downloader as kb
import pandas as pd
import datetime

# scripts
create_eod_table_script = """
CREATE TABLE IF NOT EXISTS eod
(bar_date TEXT, 
open_price REAL,
high_price REAL,
low_price REAL,
close_price REAL,
volume INTEGER,
symbol TEXT);
"""

insert_eod_row_script = """
INSERT INTO eod  
(bar_date, open_price, high_price, low_price, close_price, 
volume, symbol)
VALUES
(?, ?, ?, ?, ?, ?, ?);
"""

select_all_script = """
SELECT * FROM eod
"""

select_symbol_range_script = """
SELECT * FROM eod 
WHERE symbol = ?
AND bar_date >= ?
AND bar_date <= ?
"""

select_days_back_script = """
SELECT * FROM eod 
WHERE symbol = ?
AND bar_date < ?
ORDER BY bar_date DESC
LIMIT ?
"""

first_date_script = """
SELECT bar_date FROM eod WHERE symbol = ? ORDER BY bar_date LIMIT 1
"""

last_date_script = """
SELECT bar_date FROM eod WHERE symbol = ? 
ORDER BY bar_date DESC LIMIT 1
"""

delete_script = """
DELETE FROM eod WHERE symbol = ?
"""


def get_stocks_eod_database_path():
    return '{}/db/stocks/daily/eod.db'.format(config.data_root)


def connect_to_eod():
    conn = sql.connect(get_stocks_eod_database_path())
    conn.execute(create_eod_table_script)
    return conn


eod_db_connection = connect_to_eod()


def convert_str_date_format(dd):
    try:
        toks = dd.split('/')
        return '{}-{}-{}'.format(toks[2], toks[0], toks[1])
    except Exception as e:
        print e.message
        print dd
        raise NotImplementedError('This error has not been handled')


def get_first_date(symbol):
    cursor = eod_db_connection.cursor()
    res = cursor.execute(first_date_script, (symbol,))
    t = res.fetchone()
    if t is None:
        return None
    if len(t) == 1:
        return dates.convert_dashed_string_to_date(t[0])
    else:
        return None


def get_last_date(symbol):
    cursor = eod_db_connection.cursor()
    res = cursor.execute(last_date_script, (symbol,))
    t = res.fetchone()
    if t is None:
        return None
    if len(t) == 1:
        return dates.convert_dashed_string_to_date(t[0])
    else:
        return None


def delete_symbol_data(symbol):
    eod_db_connection.execute(delete_script, (symbol,))
    eod_db_connection.commit()


def insert_rows(symbol, rows):
    cur = eod_db_connection.cursor()

    if len(rows) <= 1:
        return
    for row in rows[:-1]:
        tokens = row.split(',')
        cur.execute(insert_eod_row_script,
                    (convert_str_date_format(tokens[0]),
                     tokens[1],
                     tokens[2],
                     tokens[3],
                     tokens[4],
                     tokens[5],
                     symbol))

    eod_db_connection.commit()


def kb_eod_request(symbol, start_date, end_date):
    kb.log_on()
    rows = kb.request_daily_data_as_list_of_rows(
        symbol, start_date=start_date,
        end_date=end_date)
    kb.log_out()
    return rows


def convert_rows_to_df(rows):
    df = pd.DataFrame(rows)
    df.columns = ['date', 'open', 'high', 'low',
                  'close', 'volume', 'symbol']

    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)
    return df


def select_eod_rows(symbol, start_date, end_date):
    cursor = eod_db_connection.cursor()
    res = cursor.execute(select_symbol_range_script,
                         (symbol, start_date, end_date))
    rows = res.fetchall()
    return convert_rows_to_df(rows)


def fill_missing_data(symbol, number_of_days, request_date):
    if isinstance(request_date, str):
        request_date = dates.convert_dashed_string_to_date(request_date)
    first_request_date = dates.add_trading_days(request_date, number_of_days * -1)
    first_available_date = get_first_date(symbol)
    last_available_date = get_last_date(symbol)
    # fill missing data if any:
    if first_available_date is None:
        #start_date = first_request_date
        start_date = datetime.datetime(2014, 1, 1)
        # end_date = request_date
        end_date = datetime.datetime.now() - datetime.timedelta(days=1)
        rows = kb_eod_request(symbol, start_date, end_date)
        insert_rows(symbol, rows)
    else:
        if first_request_date < first_available_date:
            #start_date = first_request_date
            start_date = datetime.datetime(2014, 1, 1)
            #end_date = dates.add_trading_days(first_available_date, -1)
            end_date = datetime.datetime.now() - datetime.timedelta(days=1)
            rows = kb_eod_request(symbol, str(start_date), str(end_date))
            insert_rows(symbol, rows)
        if request_date > last_available_date:
            #start_date = dates.add_trading_days(last_available_date, 1)
            start_date = datetime.datetime(2014, 1, 1)
            #end_date = request_date
            end_date = datetime.datetime.now() - datetime.timedelta(days=1)
            rows = kb_eod_request(symbol, start_date, end_date)
            insert_rows(symbol, rows)


# called by adv and adr functions
# returns a df of the data requested
def request_eod_data(symbol, number_of_days, request_date):
    if not isinstance(request_date, datetime.datetime):
        request_date = dates.convert_dashed_string_to_date(request_date)

    first_request_date = dates.add_trading_days(request_date, number_of_days * -1)
    last_request_date = dates.add_trading_days(request_date, -1)
    first_available_date = get_first_date(symbol)
    last_available_date = get_last_date(symbol)


    if first_available_date is None:
        start_date = first_request_date
        end_date = last_request_date
        rows = kb_eod_request(symbol, start_date, end_date)
        insert_rows(symbol, rows)
    else:
        if first_request_date < first_available_date:
            start_date = first_request_date
            end_date = dates.add_trading_days(first_available_date, -1)
            rows = kb_eod_request(symbol, start_date, end_date)
            insert_rows(symbol, rows)
        if last_request_date > last_available_date:
            start_date = dates.add_trading_days(last_available_date, 1)
            end_date = last_request_date
            rows = kb_eod_request(symbol, start_date, end_date)
            insert_rows(symbol, rows)

    return select_eod_rows(symbol, first_request_date, last_request_date)


def average_daily_range(symbol, look_back, date):
    try:
        df = get_eod_bars(symbol, look_back, date)
        df['range'] = df['high'] - df['low']
        return df['range'].mean()
    except:
        print 'data not available for ', symbol, look_back, date
        return 0


def average_daily_volume(symbol, look_back, date):
    df = get_eod_bars(symbol, look_back, date)
    return df['volume'].mean()


def get_db_row_count():
    cursor = eod_db_connection.cursor()
    results = cursor.execute('SELECT COUNT(*) FROM eod')
    return results.fetchone()[0]


# returns days_back number of bars from the request date
# not including request date
def get_eod_bars(symbol, days_back, request_date):
    cursor = eod_db_connection.cursor()
    results = cursor.execute(select_days_back_script,
                             (symbol, request_date, days_back))
    rows = results.fetchall()
    if len(rows) == days_back:
        return convert_rows_to_df(rows)
    else:
        fill_missing_data(symbol, days_back * 2, request_date)
        results = cursor.execute(select_days_back_script,
                             (symbol, request_date, days_back))
        rows = results.fetchall()
        if len(rows) == days_back:
            return convert_rows_to_df(rows)
        else:
            raise LookupError('Data not available.')
