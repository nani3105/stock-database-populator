from pandas import Series
from pandas_datareader.iex import IEX
from pandas.io.common import urlencode
import json

class IEXEarnings(IEX):

    def __init__(self, symbols=None, start=None, end=None, retry_count=3,
                 pause=0.1, session=None):
        super(IEXEarnings, self).__init__(symbols=symbols,
                                  start=start, end=end,
                                  retry_count=retry_count,
                                  pause=pause, session=session)

    @property
    def url(self):
        """API URL"""
        qstring = urlencode(self._get_params(self.symbols))
        return "https://cloud.iexapis.com/stable/stock/{}/earnings/6?{}".format(self.service,
                                                             qstring)

    @property
    def service(self):
        """Service endpoint"""
        return self.symbols[0]

    def _get_params(self, symbols):
        return {'token': 'sk_2eec5a49b4654299b50e3011eb2e9308'}

    def _read_one_data(self, url, params):
        resp = self._get_response(url, params=params)
        jsn = json.loads(resp.text)

        e = jsn['earnings']
        df = Series(e)
        return df
