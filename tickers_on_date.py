# -*- coding: utf-8 -*-

import tickers_process
import pandas as pd


def tickersondate(input_date, tickers_str):
    return_data = pd.DataFrame([])
    for ticker in tickers_str.split(','):
        # noinspection PyBroadException
        try:
            tickers = tickers_process.getProfitForTickerInRange(ticker, input_date, input_date, False)
            return_data = return_data.append(pd.DataFrame(tickers))
        except Exception as e:
            print(e)
        if len(return_data) == 0:
            raise Exception('your data frame is empty')
    return return_data

