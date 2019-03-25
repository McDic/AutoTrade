"""
<module AutoTrade.connection.http.cryptocompare>
HTTP connection for cryptocompare.com to get historical data.
"""
# ----------------------------------------------------------------------------------------------------------------------
# Libraries

# Standard libraries
import time

# External libraries

# Custom libraries
from connection.http.base import AbstractHTTPConnection

# ----------------------------------------------------------------------------------------------------------------------
# CryptoCompare

class CryptoCompare(AbstractHTTPConnection):
    """
    <class CryptoCompare>
    Used to interact between client and CryptoCompare.com to get historical price data.
    """

    def __init__(self, api_key):
        """
        <method CryptoCompare.__init__>
        :param api_key: API key for CryptoCompare.
        """

        # Parent class initialization
        super().__init__(connectionName = "CryptoCompare data loader", keys= {"api": api_key},
                         baseURL = "https://min-api.cryptocompare.com",
                         callLimits = {"default": (60*60*24*31, 10**5)})

        # API key register on session header
        self.session.headers["authorization"] = "Apikey " + self.key["api"]

    def rateLimit(self):
        """
        <method CryptoCompare.rateLimit>
        Used to get current rate limit.
        ex) https://min-api.cryptocompare.com/stats/rate/limit
        :return: Request object
        """

        # Create request and return
        req = self.request("GET", "stats/rate/limit", "default", 1)
        return req

    def historicalMinuteOHLCV(self, baseCurrency, targetCurrency,
                              startTimestamp: int, endTimestamp: int,
                              exchange = "CCCAGG"):
        """
        <method CryptoCompare.historicalMinuteOHLCV>
        Used to get historical minute data.
        ex) https://min-api.cryptocompare.com/data/histominute?fsym=ETH&tsym=BTC&limit=3&aggregate=1&toTs=1544815980
        :param baseCurrency:    Base currency to search. ex) USD, KRW, BTC, USDT
        :param targetCurrency:  Quote currency to search. ex) BTC, ETH, XRP
        :param startTimestamp:  Start timestamp.
        :param endTimestamp:    End timestamp. Search [startTimestamp, endTimestamp]
        :param exchange:        Exchange. CCCAGG is the default value of CryptoCompare.com
        :return: Request object
        """

        # Round timestamps
        assert isinstance(startTimestamp, int) and isinstance(endTimestamp, int)
        if startTimestamp % 60: startTimestamp += 60 - startTimestamp % 60
        endTimestamp -= endTimestamp % 60

        # Create request and return
        req = self.request("GET", "data/histominute", "default", 1,
                           params = {"tryConversion": "false", "fsym": baseCurrency, "tsym": targetCurrency,
                                     "limit": (endTimestamp - startTimestamp) // 60 + 1,
                                     "e": exchange, "toTs": endTimestamp})
        return req


if __name__ == "__main__":
    with open("cryptocompare.key") as cckeyFile: key = cckeyFile.read()
    cc = CryptoCompare(key)
    # req = cc.historicalMinuteOHLCV("BTC", "ETH", round(time.time() - 60*60), round(time.time()))
    req = cc.rateLimit()
    resp = req.result()
    print(resp.url)
    print(resp.json())
