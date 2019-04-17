"""
<module AutoTrade.connection.http.cryptocompare>
HTTP connection for cryptocompare.com to get historical data.
"""
# ----------------------------------------------------------------------------------------------------------------------
# Libraries

# Standard libraries
import asyncio
import time
from pprint import pprint

# External libraries

# Custom libraries
from connection.http.base import AbstractHTTPConnectionClass, AbstractHTTPConnection

# ----------------------------------------------------------------------------------------------------------------------
# CryptoCompare

class CryptoCompareClass(AbstractHTTPConnectionClass):
    """
    <class CryptoCompare>
    Used to interact between client and CryptoCompare.com to get historical price data.
    """

    def __init__(self):
        """
        <method CryptoCompare.__init__>
        :param api_key: API key for CryptoCompare.
        """

        # Parent class initialization
        super().__init__(connectionName = "CryptoCompare Abstract Connection",
                         baseURL = "https://min-api.cryptocompare.com")


    def getRateLimit(self):
        """
        <method CryptoCompare.getRateLimit>
        Used to get current rate limit information
        ex) https://min-api.cryptocompare.com/stats/rate/limit
        :return: Request object coroutine
        """

        # Create request and return
        req = self.request("GET", "stats/rate/limit")
        return req

    def historicalMinuteOHLCV(self, baseCurrency: str, targetCurrency: str,
                              startTimestamp: int, endTimestamp: int,
                              exchange = "CCCAGG"):
        """
        <method CryptoCompare.historicalMinuteOHLCV>
        Used to get historical minute data.
        ex) https://min-api.cryptocompare.com/data/histominute?fsym=ETH&tsym=BTC&limit=3&aggregate=1&toTs=1544815980
        :param baseCurrency:    Base currency to search. ex) USD, KRW, BTC, USDT
        :param targetCurrency:  Quote currency to search. ex) BTC, ETH, XRP
        :param startTimestamp:  Start timestamp.
        :param endTimestamp:    End timestamp. Search [startTimestamp, endTimestamp] but no more than 2000 data
        :param exchange:        Exchange. CCCAGG is the default value of CryptoCompare.com
        :return: Request object
        """

        # Round timestamps
        if not isinstance(startTimestamp, int) or not isinstance(endTimestamp, int):
            raise TypeError("Both timestamps must be type of int")

        if startTimestamp % 60: startTimestamp += 60 - startTimestamp % 60
        endTimestamp -= endTimestamp % 60

        # Check limit info
        if (endTimestamp - startTimestamp)/60 > 2000:
            raise ValueError("You can't fetch data more the 2000")
        limit = (endTimestamp - startTimestamp) // 60 + 1


        # Create request coroutine and return
        req = self.request("GET", "data/histominute",
                           params = {"tryConversion": "false", "fsym": baseCurrency, "tsym": targetCurrency,
                                     "limit": limit,
                                     "e": exchange, "toTs": endTimestamp})
        return req

async def CryptoCompare(api_key: str):
    """
    <cryptocompare.CryptoCompare>
    Async binder of CryptoCompareClass
    :param api_key: api key for cryptocompare authorization
    :return: CryptoCompareClass initialized
    """
    header = {}
    header.update({'authorization':'Apikey{'+api_key+'}'})
    connection = CryptoCompareClass()
    await AbstractHTTPConnectionClass._init_async(connection,headers=header)
    return connection

if __name__ == "__main__":
    async def main():
        with open("cryptocompare.key") as cckeyFile: key = cckeyFile.read()
        connect = await CryptoCompare(key)
        # req = await connect.getRateLimit()
        # print(await req.json())
        req = await connect.historicalMinuteOHLCV('BTC','USD',int(time.time())-60*10,int(time.time()))
        print(int(time.time()))
        pprint(await req.json())


        # req = cc.historicalMinuteOHLCV("BTC", "ETH", round(time.time() - 60*60), round(time.time()))
        # req = cc.rateLimit()
        # resp = req.result()
        # print(resp.url)
        # print(resp.json())

    asyncio.get_event_loop().run_until_complete(main())
