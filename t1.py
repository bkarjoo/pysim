import pandas as pd
import pickle
import sqlite3

# conn = sqlite3.connect("minute.db")
# cur.execute("CREATE TABLE IF NOT EXISTS bars ('date_time','open','high','low','close','volume','symbol')")
file_handler = open('f:/custom/Book1.csv', 'r')
r = file_handler.read()
symbols = r.split('\n')
file_handler.close()

for symbol in symbols:
    print symbol
    path = 'f:/kibot_data/stocks/minute/{}.txt'.format(symbol)
    headers = ['date_time', 'open', 'high', 'low', 'close', 'volume']
    dtypes = {'date_time': 'str', 'open': 'float', 'high': 'float', 'low': 'float', 'close': 'float',
              'volume': 'object'}
    parse_dates = ['date_time']
    df = pd.read_csv(path, names=headers, dtype=dtypes, parse_dates=parse_dates,
                     index_col='date_time')

    # path = 'f:/kibot_data/stocks/pickle/{}.pkl'.format(symbol)
    # f = open(path,'rb')
    # df = pickle.load(f)
    # f.close()

    df['symbol'] = symbol

    years = ['2017', '2018']
    months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']

    for y in years:
        for m in range(1, 13):
            m1str = '0{}'.format(str(m)) if m < 10 else str(m)
            m2 = m + 1 if m < 12 else 1

            m2str = '0{}'.format(str(m2)) if m2 < 10 else str(m2)

            df_temp = df['{}-{}-01'.format(y, m1str):'{}-{}-01'.format(int(y) + 1 if m2 == 1 else y, m2str)]

            #             file_handler = open('f:/kibot_data/stocks/pickle/{}/{}/{}.pkl'.format(y, str(m), symbol), 'wb')
            #             pickle.dump(df_temp, file_handler)
            #             file_handler.close()

            conn = sqlite3.connect('m{}{}.db'.format(y, m1str))
            cur = conn.cursor()
            cur.execute("CREATE TABLE IF NOT EXISTS bars ('date_time','open','high','low','close','volume','symbol')")
            df_temp.to_sql('bars', conn, if_exists='append', index=True)
            conn.close()