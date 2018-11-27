import holidays
import datetime


us_holidays = holidays.UnitedStates()


def is_weekend(dt):
    return dt.weekday() == 5 or dt.weekday() == 6


def market_closed(dt):
    condition1 = is_weekend(dt)
    condition2 = False
    if dt in us_holidays:
        if us_holidays[dt][:3] == 'Vet' or us_holidays[dt][:3] == 'Col':
            condition2 = True
    return condition1 or condition2


def is_half_day(dt):
    # TODO implement
    pass


# traverses days until the required number of days are added to the date
# if number_of_trading_days is negative it goes backwards
# returns the date of the final trading date reached
def add_trading_days(start_date, number_of_trading_days):
    # TODO implement

    if isinstance(start_date, datetime.date):
        start_date = datetime.datetime(start_date.year, start_date.month, start_date.day)
    trading_days_found = 0
    increment = 1 if number_of_trading_days > 0 else -1
    if not isinstance(start_date, datetime.datetime):
        i = convert_dashed_string_to_date(start_date)

    else:
        i = start_date

    #print 'start_date', i
    while True:
        i += datetime.timedelta(days=increment)
        #print 'back', i
        if not market_closed(i):

            trading_days_found += 1
            #print 'increment', trading_days_found
        if trading_days_found == abs(number_of_trading_days):
            break
    return i.date()


def number_of_trading_days(start_date, end_date):
    if isinstance(start_date, str):
        start_date = convert_dashed_string_to_date(start_date)
    if isinstance(end_date, str):
        end_date = convert_dashed_string_to_date(end_date)
    i = start_date
    n = 0 if market_closed(i) else 1
    while i < end_date:
        i += datetime.timedelta(days=1)
        if not market_closed(i):
            # print i
            n += 1
    return n


# takes yyyy-mm-dd
def convert_dashed_string_to_date(date_str):
    try:
        toks = date_str.split('-')
        y = int(toks[0])
        m = int(toks[1])
        d = int(toks[2][:2])
        return datetime.datetime(y, m, d).date()
    except Exception as e:
        print __name__, 'date conversion failure', e.message


def convert_slashed_string_to_date(date_str):
    try:
        toks = date_str.split('/')
        return '{}-{}{}-{}{}'.format(toks[2],
                                 '0' if len(toks[0]) < 2 else '',
                                 toks[0],
                                 '0' if len(toks[0]) < 2 else '',
                                 toks[1])
    except:
        return None


