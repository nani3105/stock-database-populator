import json
import datetime
from pandas_datareader import data as pdr
from pandas_datareader import nasdaq_trader as nt
from pandas_datareader._utils import RemoteDataError
from pymongo import MongoClient
import dns
import multiprocessing
import fix_yahoo_finance as yf



yf.pdr_override()


START_DATE = "2016-01-01"
END_DATE = datetime.datetime.today().strftime('%Y-%m-%d')

client = MongoClient('mongodb+srv://admin:Waddsad123@cluster0-i5k0z.gcp.mongodb.net/test?retryWrites=true&w=majority', connect= False)
db_stocks = client.stocks


def func(quotes):
    for index, quote in quotes.iterrows():
        ticker = quote['NASDAQ Symbol']
        stock = {
            'symbol': ticker,
            'name': quote['Security Name'],
            'etf': quote['ETF'],
        }

        historical_prices = []
        print(f"pulling historical prices from yahoo for ticker {ticker}")
        all_data = None
        try:
            all_data = pdr.get_data_yahoo([ticker], START_DATE, END_DATE)
        except RemoteDataError:
            print(f"Failed to pull info from yahoo, ticker [{ticker}]")

        if all_data is not None:
            for idx, ohlc in all_data.iterrows():
                open, close, high, low = ohlc[['Open', 'Close', 'High', 'Low']]
                historical_prices.append({
                    'open': round(open, 2),
                    'high': round(high, 2),
                    'close': round(close, 2),
                    'low': round(low, 2),
                    'date': idx.strftime('%Y-%m-%d')
                })
        stock['historical_prices'] = historical_prices

        result = db_stocks.ticker.insert_one(stock)
        print('One stock: {0}'.format(result.inserted_id))
    return quotes


def getStockQuotes():
    quotes = nt.get_nasdaq_symbols()

    num_processes = multiprocessing.cpu_count() - 1
    chunk_size = int(quotes.shape[0]/num_processes)
    chunks = [quotes.ix[quotes.index[i: i + chunk_size]] for i in range(0, quotes.shape[0], chunk_size)]

    pool = multiprocessing.Pool(processes=num_processes)
    pool.map(func, chunks)


getStockQuotes()
