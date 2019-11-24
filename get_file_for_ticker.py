# -*- coding: utf-8 -*-
import os
import urllib3
import tickers_process


def getfileforTicker(ticker_name, from_date, to_date, path_filename, form):
    if not os.path.isdir(path_filename):
        full_path = '{}.{}'.format(path_filename, form)
    else:
        full_path = os.path.join('{}.{}'.format(path_filename, form))
    http = urllib3.PoolManager()
    response = http.request('GET', tickers_process.generate_api_url(ticker_name, from_date, to_date))
    with open(full_path, 'wt') as f:
        f.write(str(response.data, 'utf-8'))

