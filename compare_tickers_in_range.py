import tickers_process
import numpy
import pandas as pd


def comparetickersinrange(ticker_list, from_date, to_date):
    return_data = pd.DataFrame([])
    for ticker in ticker_list.split(','):
        accumulated = tickers_process.getP2vForTickerInRange(ticker, from_date, to_date)
        profit_data_frame = tickers_process.getProfitForTickerInRange(ticker, from_date, to_date, accumulated=False)
        profit = profit_data_frame.loc[int(len(profit_data_frame)), 'Close'] / profit_data_frame.loc[1, 'Close']
        accumulated['avg'] = numpy.mean(profit_data_frame['Close'])
        accumulated['std'] = numpy.std(profit_data_frame['Close'])
        accumulated['profit'] = profit
        accumulated['tickerName'] = ticker
        return_data = return_data.append(pd.DataFrame(accumulated))
        return_data = return_data.drop('days_max_min', 1)
    return return_data
