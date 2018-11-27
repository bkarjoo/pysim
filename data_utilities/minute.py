# single day access only
import sqlite3
import config
import trade_utilities.dates as dates
import kibot.kibot_downloader as kb
import io
import pandas as pd
import datetime as dt

# scripts, symbol is needed because bars are combined
create_eod_table_script = """
CREATE TABLE IF NOT EXISTS bars
(date_time TEXT, 
open_price REAL,
high_price REAL,
low_price REAL,
close_price REAL,
volume INTEGER,
symbol TEXT);
"""

insert_row_query = """
INSERT INTO bars
(date_time, open_price, high_price, low_price, close_price, volume, symbol)
VALUES
(?,?,?,?,?,?,?)
"""

count_query = """
SELECT COUNT(*) FROM bars
"""

get_symbols_for_date_query = """
SELECT DISTINCT symbol FROM bars WHERE DATE(date_time) = ?
"""

select_all_query = """
SELECT * FROM bars
"""

select_all_for_date_query = """
SELECT * FROM bars WHERE DATE(date_time) = ?
"""

select_basket_for_date_query = """
SELECT * FROM bars
WHERE DATE(date_time) = ?
AND symbol IN ({})
ORDER BY date_time
"""

active_connection = None
active_connection_path = ''


# used to make ?,?,...,? string for the number of symbols
# the result is inserted into query
def construct_select_basket_query(symbols):
    qms = ','.join('?' * len(symbols))
    return select_basket_for_date_query.format(qms)


# path depends on date, each month year has its own db
def db_path(date):
    if isinstance(date, str):
        date = dates.convert_dashed_string_to_date(date)
    return '{}/db/stocks/minute/{}/minute{}{}{}.db'.format(
        config.data_root, date.year, date.year,
        '0' if date.month < 10 else '', date.month)


# connects to proper year/month db
def switch_active_connection(date):
    global active_connection
    global active_connection_path
    new_path = db_path(date)
    if active_connection_path != new_path:
        if active_connection is not None:
            active_connection.close()
        active_connection_path = new_path
        active_connection = sqlite3.connect(active_connection_path)
        active_connection.execute(create_eod_table_script)


# converts query result rows to a dataframe with datetime conversion
def convert_db_rows_to_df(rows):
    df = pd.DataFrame(rows)
    df.columns = ['date_time', 'open', 'high', 'low', 'close', 'volume', 'symbol']
    df['date_time'] = pd.to_datetime(df['date_time'])
    df.set_index('date_time', inplace=True)
    return df


# this is to check which symbols have data
def get_symbols_for_date(date):
    switch_active_connection(date)
    cursor = active_connection.cursor()
    res = cursor.execute(get_symbols_for_date_query, (str(date.date()),))
    symbols = res.fetchall()
    # symbols is a list of tuples
    # convert to list:
    return [symbol[0] for symbol in symbols]


# response must be unprocessed
# check to make sure there's nothing there for that date
def insert_kb_response_into_database(response_text, symbol):
    rows = response_text.split('\r\n')
    cursor = active_connection.cursor()
    for row in rows[:-1]:
        tokens = row.split(',')
        if len(tokens) > 1:
            date_time = '{} {}'.format(
                dates.convert_slashed_string_to_date(tokens[0]),
                tokens[1])
            cursor.execute(insert_row_query, (date_time, tokens[2], tokens[3], tokens[4],
                                              tokens[5], tokens[6], symbol))
    active_connection.commit()


def get_number_of_rows():
    r = active_connection.cursor().execute(count_query)
    return r.fetchone()[0]


def select_all():
    r = active_connection.cursor().execute(select_all_query)
    return r.fetchall()


def select_all_for_date(date):
    r = active_connection.cursor().execute(select_all_for_date_query, (str(date.date()),))
    return r.fetchall()


def select_basket_for_date(basket, date):
    if not isinstance(basket, list):
        raise ValueError('Basket must be a list')
    query = construct_select_basket_query(basket)
    query_parameters = (str(date.date()),) + tuple(basket)
    switch_active_connection(date)
    cursor = active_connection.cursor()
    res = cursor.execute(query, query_parameters)
    return res.fetchall()


# this is the main usecase
def request_basket_data(basket, date):
    switch_active_connection(date)
    available_symbols = get_symbols_for_date(date)
    missing_symbols = list(set(basket) - set(available_symbols))
    if len(missing_symbols) > 0:
        for symbol in missing_symbols:
            print 'requesting', symbol
            response = kb.request_for_single_day(symbol, date)
            insert_kb_response_into_database(response, symbol)
    rows = select_basket_for_date(basket, date)
    return convert_db_rows_to_df(rows)

# print request_basket_data(['AAPL', 'MSFT', 'C'], dt.datetime(2018,1,2))
