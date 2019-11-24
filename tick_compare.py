# -*- coding: utf-8 -*-

import tickers_process
import pandas as pd


def tickcompare(tickers, from_date, to_date, data_type):
    data_frame = pd.DataFrame([])
    for ticker in tickers.split(','):
        data_t = tickers_process.getDataForTickerInRange(ticker, from_date, to_date, data_type)
        data_frame = data_frame.append(pd.DataFrame(data_t))
    summarize = data_frame.groupby(['Date', 'TickerName']).sum()
    if summarize.size < 0:
        print("nothing to show :( ")
        return
    summarize.unstack().T.plot(kind='bar')

