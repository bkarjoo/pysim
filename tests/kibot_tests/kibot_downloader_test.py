from kibot.kibot_downloader import *
import datetime


def log_on_test():
    return log_on()


def log_out_test():
    return log_out()


def kibot_download_test():
    # request data for las t10 ays
    r = request_history('AAPL', '1', None, '2018-10-26', '2018-10-26')
    l = r.text.split('\r\n')
    return len(l) == 776


def request_adjustments_test():
    r = request_adjustments('AAPL')
    return len(r.text.split('\r\n')) > 20


def request_history_as_data_frame_test():
    df = request_history_as_data_frame('AAPL', '1', None, '2018-10-26', '2018-10-26')
    return len(df) == 775


def request_daily_data_test():
    df = request_daily_data('AAPL', 'daily', None, '2017-10-26', '2018-10-26')
    return len(df) == 253


# print get_pickle_path('AAPL', datetime.datetime(2017, 10, 26))

# print log_on_test(), ': log_on_test'
# print kibot_download_test(), ': kibot_download_test'
# print request_adjustments_test(), ': request_adjustments_test'
# print request_history_as_data_frame_test(), ': request_history_as_data_frame_test'
# print request_daily_data_test(), ': request_daily_data_test'
# print log_out_test(), ': log_out_test'
