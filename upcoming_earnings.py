from yahoo_earnings_calendar import YahooEarningsCalendar
import datetime
import iex_earnings
import pprint

from pymongo import MongoClient

client = MongoClient('mongodb+srv://admin:Waddsad123@cluster0-i5k0z.gcp.mongodb.net/test?retryWrites=true&w=majority', connect= False)
tickers = client.stocks.ticker

date_from = datetime.datetime.today()
date_to = datetime.datetime.today() + datetime.timedelta(weeks=1)

yec = YahooEarningsCalendar()

earnings = yec.earnings_between(date_from, date_to)

errors = []

for earning in earnings:
    ticker = earning['ticker']
    stock = tickers.find_one({'symbol': ticker})

    if stock is not None:
        series = iex_earnings.IEXEarnings([ticker]).read()
        s = {
            'ticker': ticker
        }
        past_earnings = []
        for row in series:
            announceTime = row['announceTime']
            earningDate = row['EPSReportDate']

            past_earning = {
                'announceTime': announceTime,
                'earningsDate': earningDate
            }

            priceChange, priceChangePercent = 0.00, 0.00
            for idx, ohlc in enumerate(stock['historical_prices']):
                if ohlc['date'] == earningDate:
                    if announceTime == 'AMC':
                        priceChange = round(ohlc['close'] - stock['historical_prices'][idx + 1]['open'], 2)
                        priceChangePercent = round((priceChange/ohlc['close']) * 100, 2)
                        break
                    elif announceTime == 'BTO':
                        priceChange = round(stock['historical_prices'][idx - 1]['close'] - ohlc['open'], 2)
                        priceChangePercent = round((priceChange/stock['historical_prices'][idx - 1]['close']) * 100, 2)
                        break
            past_earning['priceChange'] = priceChange
            past_earning['priceChangePercent'] = priceChangePercent
            past_earnings.append(past_earning)
        s['past_earnings'] = past_earnings

        pprint.pprint(s)

    else:
        errors.append(ticker)

print(errors)
