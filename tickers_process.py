# -*- coding: utf-8 -*-

import os
import urllib3
import itertools
import pandas as pd
import numpy as np
import datetime
from datetime import timedelta


####################
# #####utils###### #
####################


def is_valid_dates(from_date, to_date):
    str_format = '%Y-%m-%d'
    from_date = datetime.datetime.strptime(from_date, str_format)
    to_date = datetime.datetime.strptime(to_date, str_format)
    return (to_date - from_date).days > 0


def validate_data_type(type):
    try:
        valid_types = ['open', 'close', 'hight', 'low', 'volume']
        if not str(type).lower() in valid_types:
            raise Exception("invalid data type")
    except Exception:
        print("data type is not valid")


def generate_api_url(ticker_name, from_date, to_date):
    with open("api.key.txt", mode="rt", encoding="utf8") as f:
        key = f.read()
    url = 'https://www.quandl.com/api/v3/datasets/EOD/{}.csv?start_date={}&end_date={}&api_key={}'.format(
        ticker_name, from_date, to_date, key)
    return url


def get_online_data(ticker_name, from_date, to_date):
    urllib3.disable_warnings()
    http = urllib3.PoolManager()
    response = http.request('GET', generate_api_url(ticker_name, from_date, to_date))
    try:
        # start to parse data
        data_string = str(response.data)
        # first varification if data is available
        if 'incorrect' not in data_string:
            # if we here its mean we ok
            with open(os.path.join('.', 'data', '{}_{}_{}.csv'.format(ticker_name.lower(), from_date.replace('-', ''),
                                                                    to_date.replace('-', ''))), 'wt') as file:
                file.write(str(response.data, 'utf-8'))
            return
        else:
            # if not, we print error to  user
            print('Your Ticker Name not exist')
            return
    except Exception as E:
        print(E)
        return


def parse_file_format(name, from_date, to_date):
    return '{}_{}_{}.csv'.format(name, from_date.replace('-', ''), to_date.replace('-', ''))


def extract_dates_meta_data(existing_to_date_format, existing_from_date_format, from_date_str, to_date_str):
    existing_to_date = datetime.datetime.strftime(existing_to_date_format, '%Y-%m-%d')
    existing_from_date = datetime.datetime.strftime(existing_from_date_format, '%Y-%m-%d')

    # find specific dates
    # edge to date
    to_date_edge = (existing_to_date_format + timedelta(days=1)).strftime('%Y-%m-%d')
    # edge from date
    from_date_edge = (existing_from_date_format - timedelta(days=1)).strftime('%Y-%m-%d')

    # required dates
    required_from_date = datetime.datetime.strptime(from_date_str, '%Y-%m-%d')
    required_to_date = datetime.datetime.strptime(to_date_str, '%Y-%m-%d')

    # create new dates
    new_file_from_date = min(existing_from_date.replace('-', ''), from_date_str.replace('-', ''))
    new_file_to_date = max(existing_to_date.replace('-', ''), to_date_str.replace('-', ''))

    if required_from_date < existing_from_date_format and required_to_date <= existing_to_date_format:
        query_from_date = from_date_str
        query_to_date = from_date_edge
    elif required_from_date >= existing_to_date_format and required_to_date > existing_to_date_format:
        query_from_date = to_date_edge
        query_to_date = to_date_str
    elif required_from_date < existing_from_date_format < required_to_date <= existing_to_date_format:
        query_from_date = from_date_str
        query_to_date = from_date_edge
    elif existing_from_date_format <= required_from_date < existing_to_date_format < required_to_date:
        query_from_date = to_date_edge
        query_to_date = to_date_str
    elif required_from_date >= existing_from_date_format and required_to_date <= existing_to_date_format:
        print('you have the data in existing file')
    elif required_from_date < existing_from_date_format and required_to_date > existing_to_date_format:
        query_from_date = from_date_str
        query_to_date = from_date_edge
    return query_from_date, query_to_date, new_file_from_date, new_file_to_date


def generate_data_frame(data_list):
    # manipulate file names to concrete data frame
    file_list = [s.strip('.csv') for s in data_list]
    file_list = [i.split('_') for i in file_list]
    merged = list(itertools.chain(*file_list))
    data_frame = pd.DataFrame(np.array(merged).reshape(int((len(merged)) / 3), 3),
                              columns=('ticker_name', 'from_date', 'to_date'))
    data_frame['from_date'] = pd.to_datetime(data_frame['from_date'])
    data_frame['to_date'] = pd.to_datetime(data_frame['to_date'])
    return data_frame


######################
### the functions ####
######################

def fetchTicker(ticker_name, from_date_str, to_date_str):
    try:
        if not is_valid_dates(from_date_str, to_date_str):
            print("bad dates, try again")
            return

        # initiate data dir
        if not os.path.exists('data'):
            print("data dir is not exist")
            os.mkdir('data')
        data_list = os.listdir('data')
        # check all available data
        my_tickers = generate_data_frame(data_list)
        # manipulate string
        ticker_label = str(ticker_name.lower())

        # check if we do not have ticker at all, we need full fetch :
        if ticker_label in my_tickers.values:

            # find data that already exists
            my_ticker_data_frame = my_tickers.loc[my_tickers['ticker_name'] == ticker_label]

            # existing dates formatted

            existing_to_date_format = my_ticker_data_frame['to_date'].iloc[0]
            existing_from_date_format = my_ticker_data_frame['from_date'].iloc[0]

            existing_to_date = datetime.datetime.strftime(existing_to_date_format, '%Y-%m-%d')
            existing_from_date = datetime.datetime.strftime(existing_from_date_format, '%Y-%m-%d')

            # find specific dates
            # edge to date
            to_date_edge = (existing_to_date_format + timedelta(days=1)).strftime('%Y-%m-%d')

            # required dates
            required_from_date = datetime.datetime.strptime(from_date_str, '%Y-%m-%d')
            required_to_date = datetime.datetime.strptime(to_date_str, '%Y-%m-%d')

            (query_from_date, query_to_date, new_file_from_date, new_file_to_date) = extract_dates_meta_data(
                existing_to_date_format, existing_from_date_format, from_date_str, to_date_str)

            print("partially fetch for ticker {} in dates : {} - {}".format(ticker_name, query_from_date, query_to_date))
            get_online_data(ticker_name, query_from_date, query_to_date)

            new_file_path = os.path.join('.', 'data', parse_file_format(ticker_label, new_file_from_date,
                                                                        new_file_to_date))

            new_data_path = parse_file_format(ticker_label, query_from_date, query_to_date)
            local_data_path = parse_file_format(ticker_label, existing_from_date, existing_to_date)

            if required_from_date < existing_from_date_format and required_to_date > existing_to_date_format:
                latest_data_path = os.path.join('.', 'data', parse_file_format(ticker_label, to_date_edge, to_date_str))
                latest_data = pd.read_csv(latest_data_path)
                new_data = pd.read_csv(os.path.join('.', 'data', new_data_path))
                local_data = pd.read_csv(os.path.join('.', 'data', local_data_path))
                # concat data
                merged = pd.concat([latest_data, new_data, local_data])
                merged.to_csv(new_file_path, index=False)
                print("clean data dir from unnecessary files")
                os.remove(os.path.join('.', 'data', local_data_path))
                os.remove(os.path.join('.', 'data', new_data_path))
                os.remove(os.path.join('.', 'data', parse_file_format(ticker_label, to_date_edge, to_date_str)))
            else:
                new_data = pd.read_csv(os.path.join('.', 'data', new_data_path))
                local_data = pd.read_csv(os.path.join('.', 'data', local_data_path))
                # concat data
                merged = pd.concat([new_data, local_data])
                merged.to_csv(new_file_path, index=False)
                print("clean data dir from unnecessary files")
                os.remove(os.path.join('.', 'data', new_data_path))
                os.remove(os.path.join('.', 'data', local_data_path))
        else:
            # full fetch
            print("full fetch for ticker {}".format(ticker_name))
            get_online_data(ticker_name, from_date_str, to_date_str)
    except Exception as e:
        print(e)


def getProfitForTickerInRange(ticker_name, from_date, to_date, accumulated=False):
    print("get profit for {} ".format(ticker_name))
    day_before_from_date = (datetime.datetime.strptime(from_date, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
    data_at_close = getDataForTickerInRange(ticker_name, day_before_from_date, to_date, 'close')
    data_at_close['Date'] = pd.to_datetime(data_at_close.Date)
    sorted_data = data_at_close.sort_values(by=['Date'])
    sorted_data['profit'] = int(1)
    sorted_data = sorted_data.reset_index(drop=True)
    for i in range(1, int(len(sorted_data))):
        sorted_data.loc[i, 'profit'] = sorted_data.loc[i, 'Close'] / sorted_data.loc[i - 1, 'Close']
    if not accumulated:
        print("not accumulated data")
        return sorted_data.drop(0)
    else:
        print("accumulated data")
        sorted_data['accumulated'] = int(1)
        for i in range(1, int(len(sorted_data))):
            sorted_data.loc[i, 'accumulated'] = (sorted_data.loc[i, 'profit'] * sorted_data.loc[i - 1, 'profit']) - 1
        sorted_data = sorted_data.iloc[2:]
        sorted_data = sorted_data.drop('profit', 1)
        return sorted_data


def getDataForTickerInRange(ticker_name, from_date, to_date, data_types):
    # verify file exists
    if not os.path.exists('data'):
        os.mkdir('data')
    data_types = data_types.split(",")
    full_data_types = ['Date']
    for data_type in data_types:
        full_data_types.append(data_type[0].upper() + data_type[1:].lower())
    # get all data available
    data_list = os.listdir('data')
    # generate data frame from given files
    data_frame = generate_data_frame(data_list)
    # transform data to lower case
    my_ticker = ticker_name.lower()
    # parse date
    from_date_obj = datetime.datetime.strptime(from_date, '%Y-%m-%d')
    to_date_obj = datetime.datetime.strptime(to_date, '%Y-%m-%d')
    # iterate data in data frame
    if my_ticker in data_frame.values:
        # retrieve specific ticker data
        my_ticker_data_frame = data_frame.loc[data_frame['ticker_name'] == my_ticker]
        exist_to_date = my_ticker_data_frame['to_date'].iloc[0]
        exist_from_date = my_ticker_data_frame['from_date'].iloc[0]
        aggregated_to_date = datetime.datetime.strftime(exist_to_date, '%Y%m%d')
        aggregated_from_date = datetime.datetime.strftime(exist_from_date, '%Y%m%d')
        path = os.path.join('.', 'data', parse_file_format(my_ticker, aggregated_from_date, aggregated_to_date))
        # checking weather we need to fetch new data or not
        # if not
        if exist_from_date <= from_date_obj < exist_to_date and to_date_obj <= exist_to_date:
            print("we dont need to fetch more data for {} :)".format(ticker_name))
            new_data_frame = pd.read_csv(path, parse_dates=['Date'])
            final = new_data_frame[full_data_types]
            final = final[(final['Date'] >= from_date_obj) & (final['Date'] <= to_date_obj)]
            final['TickerName'] = my_ticker
            return final
        # if we need to fetch
        else:
            print("we need to fetch more data for {}".format(ticker_name))
            fetchTicker(ticker_name, from_date, to_date)
            data_list = os.listdir('data')
            data_frame = generate_data_frame(data_list)
            my_ticker = ticker_name.lower()
            from_date_obj = datetime.datetime.strptime(from_date, '%Y-%m-%d')
            to_date_obj = datetime.datetime.strptime(to_date, '%Y-%m-%d')
            my_ticker_data_frame = data_frame.loc[data_frame['ticker_name'] == my_ticker]
            aggregated_to_date = datetime.datetime.strftime(my_ticker_data_frame['to_date'].iloc[0], '%Y%m%d')
            aggregated_from_date = datetime.datetime.strftime(my_ticker_data_frame['from_date'].iloc[0], '%Y%m%d')
            # create the aggregated file
            path = os.path.join('.', 'data', parse_file_format(my_ticker, aggregated_from_date, aggregated_to_date))
            new_data_frame = pd.read_csv(path, parse_dates=['Date'])
            final = new_data_frame[full_data_types]
            # clean data
            final = final[(final['Date'] >= from_date_obj) & (final['Date'] <= to_date_obj)]
            final['TickerName'] = my_ticker
            return final
    else:
        # if not exist, full fetch please
        fetchTicker(ticker_name, from_date, to_date)
        temp_df = pd.read_csv(os.path.join('.', 'data', parse_file_format(my_ticker, from_date, to_date)))
        final = temp_df[full_data_types]
        final.is_copy = False
        final['TickerName'] = my_ticker
        return final


def getP2vForTickerInRange(ticker_name, from_date, to_date):
    # get data
    profit_data = getProfitForTickerInRange(ticker_name, from_date, to_date, accumulated=False)
    # find max and min
    max_row = profit_data.loc[profit_data['Close'].idxmax()]
    min_row = profit_data.loc[profit_data['Close'].idxmin()]
    max_value = max_row['Close']
    min_value = min_row['Close']
    n = float(max_value - min_value)
    max_to_min = round(n, 2)
    days_p_v = (max_row['Date'] - min_row['Date']).days
    return pd.DataFrame({'max': [max_value], 'min': [min_value], 'max_to_min': [max_to_min], 'days_max_min': [days_p_v]})
