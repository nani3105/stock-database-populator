from yahoo_earnings_calendar import YahooEarningsCalendar
import datetime
import iex_earnings
import pprint

from pymongo import MongoClient

client = MongoClient('mongodb+srv://admin:Waddsad123@cluster0-i5k0z.gcp.mongodb.net/test?retryWrites=true&w=majority', connect= False)
tickers = client.stocks.ticker


def next_sunday(d):
    days_ahead = 6 - d.weekday()
    if days_ahead <= 0:
        days_ahead += 7
    return d + datetime.timedelta(days_ahead)


date_from = next_sunday(datetime.datetime.today()) #+ datetime.timedelta(weeks=2)
date_to = date_from + datetime.timedelta(weeks=1)

yec = YahooEarningsCalendar()

earnings = yec.earnings_between(date_from, date_to)

errors = []

earnings = [{'ticker': 'LULU'}]

for earning in earnings:
    ticker = earning['ticker']
    stock = tickers.find_one({'symbol': ticker})

    if stock is not None:
        try:
            series = iex_earnings.IEXEarnings([ticker]).read()
        except KeyError:
            print(f"Error getting the iex earnings for {ticker}")
        s = {
            'ticker': ticker
        }
        past_earnings = []
        for row in series:
            try:
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
            except:
                print(f"Error in calculating earnings {ticker}")
        s['past_earnings'] = past_earnings

        avg_move = round(sum(pe['priceChangePercent'] for pe in s['past_earnings']) / len(s['past_earnings']), 2)
        print(f"Implied Move: {ticker} - {avg_move}")
        if avg_move >= 4.5:
            print(ticker)

    else:
        errors.append(ticker)

print(errors)
