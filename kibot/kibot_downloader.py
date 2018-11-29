import requests
import config
import io
import pandas as pd
import pickle
import datetime
import sqlite3


def log_on():
    login = "http://api.kibot.com/?action=login&user={}&password={}".format(
        config.kibot_user_name, config.kibot_pass_word)
    r = requests.get(login)
    if r.text[:3] == '200' or r.text[:3] == '407':
        return True
    else:
        return False


def log_out():
    url = 'http://api.kibot.com?action=logout'
    r = requests.get(url)
    if r.text[:3] == '200' or r.text[:3] == '407':
        return True
    else:
        return False


# interval can be 1-60,120,180,240, Daily, Weekly, Monthly, Yearly
def request_history(symbol, interval, period=None, start_date=None, end_date=None,
                    regular_session=None, unadjusted=None, split_adjusted=None, the_type=None):
    url = 'http://api.kibot.com/?action=history&symbol={0}&interval={1}'.format(symbol, interval)
    if period is not None:
        url += '&period={0}'.format(period)
    if start_date is not None:
        url += '&startdate={0}'.format(start_date)
    if end_date is not None:
        url += '&enddate={0}'.format(end_date)
    if regular_session is not None:
        url += '&regularsession={0}'.format(regular_session)
    if unadjusted is not None:
        url += '&unadjusted={0}'.format(unadjusted)
    if split_adjusted is not None:
        url += '&splitadjusted={0}'.format(split_adjusted)
    if the_type is not None:
        url += '&type={0}'.format(the_type)
    return requests.get(url)


def request_adjustments(symbol=''):
    url = 'http://api.kibot.com?action=adjustments'
    if symbol != '':
        url += '&symbol={}'.format(symbol)

    return requests.get(url)


def request_history_as_data_frame(symbol, interval, period=None, start_date=None, end_date=None,
                    regular_session=None, unadjusted=None, split_adjusted=None, the_type=None):
    r = request_history(symbol, interval, period, start_date, end_date,
                    regular_session, unadjusted, split_adjusted, the_type)
    s = r.content

    headers = ['date', 'time', 'open', 'high', 'low', 'close', 'volume']
    dtypes = {'date': 'str', 'time': 'str', 'open': 'float', 'high': 'float', 'low': 'float', 'close': 'float',
              'volume': 'int'}
    parse_dates = [['date', 'time']]
    try:
        df = pd.read_csv(io.StringIO(s.decode('utf-8')), names=headers, dtype=dtypes, parse_dates=parse_dates,
                         index_col='date_time')
        df['symbol'] = symbol
        return df
    except ValueError as e:
        print 'Error loading symbol {}.\n{}'.format(symbol, e)
        return None



def get_pickle_path(symbol, date):
    if isinstance(date, datetime.datetime):
        date_str = str(date.date())
    else:
        date_str = date
    toks = date_str.split('-')
    return '{}/{}_{}{}{}.pkl'.format(config.one_day_symbol_pickle_location, symbol, toks[0], toks[1], toks[2])


def get_eod_pickle_path(symbol, date):
    date_str = str(date.date())
    toks = date_str.split('-')
    return '{}/{}_D_{}{}{}.pkl'.format(config.one_day_symbol_pickle_location, symbol, toks[0], toks[1], toks[2])




def file_exists(path):
    try:
        f = open(path)
        f.close()
        return True
    except:
        return False


def get_close(symbol, date):
    df = request_eod_data_for_one_day(symbol, date)
    return df.iloc[0]['close']


def request_eod_data_for_one_day(symbol, date):
    path = get_eod_pickle_path(symbol, date)
    if file_exists(path):
        # print 'loading pickle'
        file_handler = open(path, 'rb')
        df = pickle.load(file_handler)
        file_handler.close()
    else:
        print __name__, 'ROW 113', 'requesting data', symbol
        log_on()
        df = request_daily_data(symbol, 'daily', 1, date)
        file_handler = open(path, 'wb')
        pickle.dump(df, file_handler)
        file_handler.close()
    return df


def request_minute_data_for_one_day(symbol, date):
    path = get_pickle_path(symbol, date)
    if file_exists(path):
        # print 'loading pickle'
        file_handler = open(path, 'rb')
        df = pickle.load(file_handler)
        file_handler.close()
    else:
        print __name__, 'ROW 130', 'requesting data', symbol
        df = request_history_as_data_frame(symbol, 1, None, date, date)
        file_handler = open(path, 'wb')
        pickle.dump(df, file_handler)
        file_handler.close()
    return df


def request_minute_data_for_one_day_from_full_pickle(symbol, date):
    path = '{}/{}.pkl'.format(config.stock_pickle_path, symbol)
    if file_exists(path):
        # print 'loading pickle'
        file_handler = open(path, 'rb')
        df = pickle.load(file_handler)

        file_handler.close()

        if isinstance(date, str):
            d1 = datetime.datetime.strptime(date, '%Y-%m-%d')
        else:
            d1 = date
        df = df[d1:d1 + datetime.timedelta(days=1)]
        return df
    else:
        return None





def request_minute_data_for_one_day_from_month_pickle(symbol, date):
    path = '{}/{}/{}/{}.pkl'.format(config.stock_pickle_path, date.year, date.month, symbol)
    if file_exists(path):
        # print 'loading pickle'
        file_handler = open(path, 'rb')
        df = pickle.load(file_handler)

        file_handler.close()

        if isinstance(date, str):
            d1 = datetime.datetime.strptime(date, '%Y-%m-%d')
        else:
            d1 = date
        df = df[d1:d1 + datetime.timedelta(days=1)]
        return df
    else:
        return None



def request_minute_datas_for_one_day(symbols, date):
    # TODO delete this function, no longer used
    # get the data
    print 'requesting data for {} symbols on {}'.format(len(symbols), str(date))
    # dfs = []
    #     # i = 0
    #     # for symbol in symbols:
    #     #     i += 1
    #     #
    #     #     df = request_minute_data_for_one_day_from_month_pickle(symbol, date)
    #     #     if df is None: continue
    #     #     df['symbol'] = symbol
    #     #     dfs.append(df)
    #     #
    #     # # concat all dfs
    #     # if len(dfs) > 0:
    #     #     dfc = pd.concat(dfs)
    #     #     dfc.sort_index(inplace=True)
    #     #     return dfc
    #     # else:
    #     #     return []

    # sqlite method
    y = date.year
    #print y
    m2 = date.month
    ##print m2
    m2str = '0{}'.format(str(m2)) if m2 < 10 else str(m2)
    #print m2str
    path = '{}/stocks/db/m{}{}.db'.format(config.data_root, y, m2str)
    #print path
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    qms = ','.join('?'*len(symbols))
    next_date = date.date() + datetime.timedelta(days=1)
    query = """
    SELECT * FROM bars
    WHERE date_time >= ?
    AND date_time < ?
    --AND substr(date_time, -8) < '09:30:00'
    AND symbol IN ({})
    ORDER BY date_time
    """.format(qms)
    #print query

    tup = (str(date.date()), str(next_date)) + tuple(symbols)
    #print tup

    res = cur.execute(query, tup)

    #res = cur.execute('SELECT * FROM bars WHERE date_time < "2017-12-01" AND symbol IN ({}) ORDER BY date_time DESC LIMIT 10'.format(qms),symbols)

    dfr = pd.DataFrame(res.fetchall())
    dfr[0] = pd.to_datetime(dfr[0])
    dfr.columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'symbol']
    dfr.set_index('date', inplace=True)
    #df.columns = res.keys()
    conn.close()
    return dfr





# file_handler = open('f:/custom/Book1.csv', 'r')
# r = file_handler.read()
# symbols = r.split('\n')
# symbols = symbols[:60]
# df = request_minute_datas_for_one_day(symbols, datetime.datetime(2017,12,29))
# print len(df)
# print df


def request_daily_data(symbol, interval='daily', period=None, start_date=None, end_date=None,
                       regular_session=None, unadjusted=None, split_adjusted=None, the_type=None):
    r = request_history(symbol, interval, period, start_date, end_date,
                        regular_session, unadjusted, split_adjusted, the_type)
    s = r.content

    headers = ['date', 'open', 'high', 'low', 'close', 'volume']
    dtypes = {'date': 'str', 'open': 'float', 'high': 'float', 'low': 'float', 'close': 'float',
              'volume': 'int'}
    parse_dates = ['date']
    df = pd.read_csv(io.StringIO(s.decode('utf-8')), names=headers, dtype=dtypes, parse_dates=parse_dates,
                     index_col='date')

    return df

def request_daily_data_as_list_of_rows(symbol, interval='daily', period=None, start_date=None, end_date=None,
                       regular_session=None, unadjusted=None, split_adjusted=None, the_type=None):
    r = request_history(symbol, interval, period, start_date, end_date,
                        regular_session, unadjusted, split_adjusted, the_type)
    s = r.content
    toks = s.split('\r\n')
    return toks


def request_for_single_day(symbol, date):
    log_on()
    response = request_history(symbol, 1, None, date, date, unadjusted=1)
    return response.text

