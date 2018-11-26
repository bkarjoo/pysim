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

select_basket_script = """
SELECT * FROM bars
WHERE date_time >= ?
AND date_time < ?
AND symbol IN ({})
ORDER BY date_time
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

active_connection = None
active_connection_path = ''


def construct_select_basket_query(symbols):
    qms = ','.join('?' * len(symbols))
    return select_basket_script.format(qms)


def db_path(date):
    # path depends on date, each month year has its own db
    if isinstance(date, str):
        date = dates.convert_dashed_string_to_date(date)
    return '{}/db/stocks/minute/{}/minute{}{}{}.db'.format(
        config.data_root, date.year, date.year,
        '0' if date.month < 10 else '', date.month)


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


def data_available(symbol, date):
    pass


def insert_kb_rows_in_db(rows):
    pass


def convert_kb_rows_to_df(symbol, rows):
    s = rows
    headers = ['date', 'time', 'open', 'high', 'low', 'close', 'volume']
    dtypes = {'date': 'str', 'time': 'str', 'open': 'float', 'high': 'float', 'low': 'float', 'close': 'float',
              'volume': 'object'}
    parse_dates = [['date', 'time']]
    df = pd.read_csv(io.StringIO(s.decode('utf-8')), names=headers, dtype=dtypes, parse_dates=parse_dates,
                     index_col='date_time')
    df['symbol'] = symbol
    return df
    # TODO: who will call the df maker?


def convert_db_rows_to_df(rows):
    # TODO implement
    print rows
    print type(rows)
    # rows is a list of db rows

    # headers = ['date', 'time', 'open', 'high', 'low', 'close', 'volume', 'symbol']
    # dtypes = {'date': 'str', 'time': 'str', 'open': 'float', 'high': 'float', 'low': 'float', 'close': 'float',
    #           'volume': 'object', 'symbol': object}
    # parse_dates = [['date', 'time']]
    # df = pd.DataFrame(io.StringIO(rows.decode('utf-8')), names=headers, dtype=dtypes, parse_dates=parse_dates,
    #                  index_col='date_time')
    return None





def query_db_for_single_day(symbol, date):
    pass


def get_bars_as_df(symbol, date):
    pass


# TODO check if inmem db exists
#   db exists if the file is there
#   program should connect to all its dbs?
# TODO create an inmem minute db
#   creating the table creates the db
# TODO request a day of data from db


"""
Workflow: 

* user requests a basket of rows for a particular day
* check to see if all are available in db
* request the ones that are not available an insert in db
* query all the symbols in the basket
"""
def request_basket_data(symbols, date):
    query = construct_select_basket_query(symbols)
    print query
    next_date = date.date() + dt.timedelta(days=1)
    query_parameters = (str(date.date()), str(next_date)) + tuple(symbols)
    print query_parameters
    switch_active_connection(date)
    cursor = active_connection.cursor()
    res = cursor.execute(query, query_parameters)
    return convert_db_rows_to_df(res.fetchall())


# print request_basket_data(['AAPL', 'GE', 'IBM', 'QQQ'], dt.datetime(2018,1,2))



def kibot_request_for_single_day(symbol, date):
    kb.log_on()
    response = kb.request_history(symbol, 1, None, date, date)
    return response.text


# response must be unprocessed
# check to make sure there's nothing there for that date
def insert_kb_response_into_database(response_text, symbol):
    rows = response_text.split('\r\n')
    cursor = active_connection.cursor()
    for row in rows[:-1]:
        tokens = row.split(',')
        date_time = '{} {}'.format(
            dates.convert_slashed_string_to_date(tokens[0]),
            tokens[1])
        cursor.execute(insert_row_query, (date_time,
                                         tokens[2],
                                         tokens[3],
                                         tokens[4],
                                         tokens[5],
                                         tokens[6],
                                         symbol
                                         ))
    active_connection.commit()


def get_number_of_rows():
    r = active_connection.cursor().execute(count_query)
    return r.fetchone()[0]

#r = kibot_request_for_single_day("MSFT", dt.datetime(2018,1,2))
switch_active_connection(dt.datetime(2018,1,2))
#insert_kb_response_into_database(r, 'MSFT')
print request_basket_data(['AAPL', 'MSFT'], dt.datetime(2018,1,2))

"""TODO
# TODO 
so you have AAPL and MSFT data for this one day 
# TODO test the select basket with AAPL and MSFT
now you need to request AAPL, MSFT, GE and BAC
once you get the results you should check symbols and get notified that GE and BAC are missing
"""

"""
why should you convert date time 
it can be compared as string comparison?
get rid of the colon 
if need be it can be made an integer
"""