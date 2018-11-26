# package here the scripts for processing data
# bloomberg data
# primus data
# kibot data
# webscraping data

# data should be saved on an as needed basis
    # the adv and adr functions depend on daily data for a certain number of days
    # daily data can be loaded from kibot
    # then each row is written to db (one db per year)
    # then the data is indexed

# workflow design
    # user requests 20 day adv of AAPL adv(symbol, date, number_of_days)
        request_eod_data(symbol, start_date, end_date)
        # TODO make adv function in the functions method, can move to other places later
    # check the db to see if all those days are available (assuming that the days are sequential)
        what if there are holes? # TODO policy, db will never have any holes
        * db without holes
        * request jan 2018
        * request march 1, 2018 to march 10, 2018
        *   if last_availabe_date < first_date_requested, first_date_to_request = last_avilable_date + 1
        * request november 1, 2017, to november 20, 2017
        *   if first_available_date > last_date_requested, last_date_to_request = first_available_date - 1
        * all requested dates are added to db, if they don't exist
    # request those days
    # insert the days into db one by one if they don't exist
    # query the date range
    # rerturn the results
# TODO pickle data compilations per test in a temp file
# TODO add data request to SQLite, if data is available in the sqlite it will not be requested a second time

# data organization
# kibot_data folder is data folder root
# there are 3 types of data csv, pickle, and sqlite (csv, pkl, and db)
# next comes the instrument class (stocks, etfs, futures, forex)
# next comes the data type (daily or minute)
# then comes the year
# finally for the intraday folder you have the months
