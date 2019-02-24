"""
<module AutoTrade.connection.crypto_exchanges>
Abstract binder for all ccxt exchange sessions.
The reason why I made this is to avoid IP ban due to exceed the API call limit.
"""

# ----------------------------------------------------------------------------------------------------------------------
# Libraries

# Standard libraries
import asyncio
import atexit
import decimal

# External libraries
import ccxt.async_support as ccxt

# Custom libraries
from connection.base import AbstractConnection
import connection.errors

# ----------------------------------------------------------------------------------------------------------------------
# CCXT binder

class CCXTConnection(AbstractConnection):
    """
    <class CCXTConnection>
    CCXT exchange binder.
    """

    # Currently supporting exchanges
    supportingExchanges = {
        "Binance": ccxt.binance,
        "Bithumb": ccxt.bithumb,
        "Upbit": ccxt.upbit
    }

    # ------------------------------------------------------------------------------------------------------------------
    # Constructor

    def __init__(self, keys: dict, connectionName = "CCXT Binder"):

        # Key is passed to CCXT object instead of AbstractConnection
        super().__init__(connectionName = connectionName)

        # Register exchanges; Each exchange should be exist in CCXTConnection.supportingExchanges
        for exchangeName in keys: keys[exchangeName]["options"] = {"adjustForTimeDifference": True}
        self.exchanges = {exchangeName: CCXTConnection.supportingExchanges[exchangeName](keys[exchangeName]) for exchangeName in keys}

        # Load markets and account information
        self.markets = {} # {exchangeName: {base: {quote: {~~}}}, ...}
        asyncio.get_event_loop().run_until_complete(self.fetchMarkets())
        self.balance = {} # {exchangeName: {currency: {free: __, total: __, }}}
        asyncio.get_event_loop().run_until_complete(self.fetchBalances())

        # Register termination
        atexit.register(self.terminate)

    @staticmethod
    def makeFromFile(**filenames):
        """
        <static method CCXTConnection.makeFromFile>
        Make CCXTConnection from given file names.
        :param filenames: {ExchangeName: file, ...}
        :return: CCXTConnection Object
        """
        keys = {}
        for exchangeName in CCXTConnection.supportingExchanges:
            if exchangeName in filenames:
                with open(filenames[exchangeName]) as keyFile:
                    publicKey, privateKey = keyFile.read().split("\n")
                    keys[exchangeName] = {"apiKey": publicKey, "secret": privateKey}
        return CCXTConnection(keys)

    # ------------------------------------------------------------------------------------------------------------------
    # Termination

    async def closeExchangeSessions(self):
        """
        <async method CCXTConnection.closeExchangeSessions>
        Close all exchange sessions asynchronously. At least Upbit exchange requires to close at the end of the program.
        """
        tasks = []
        for exchangeName in self.exchanges:
            tasks.append(asyncio.create_task(self.exchanges[exchangeName].close()))
        for task in tasks: await task

    def terminate(self):
        """
        <method CCXTConnection.terminate>
        Terminate CCXTConnection.
        """
        try:
            asyncio.get_event_loop().run_until_complete(self.closeExchangeSessions()) # Close exchange sessions
        except: pass

    # __enter__ is already prepared in parent class
    def __exit__(self, exc_type, exc_val, exc_tb):
        super().__exit__(exc_type, exc_val, exc_tb)
        self.terminate()

    # ------------------------------------------------------------------------------------------------------------------
    # Helper function

    def isSupported(self, exchange: str, base: str, quote: str) -> bool:
        """
        <method CCXTConnection.isSupported>
        :return: If given (exchange, base, quote) market is supported.
        """
        try: return quote in self.markets[exchange][base]
        except KeyError: return False

    def raiseIfNotSupported(self, exchange: str, base: str, quote: str):
        """
        <method CCXTConnection.raiseIfNotSupported>
        raise Exception if given market is not supported.
        """
        if not self.isSupported(exchange, base, quote): raise connection.errors.MarketNotSupported(exchange, base, quote)

    @staticmethod
    def makeDecimal(value):
        if value is None: return decimal.Decimal(0)
        elif isinstance(value, decimal.Decimal): return value
        elif isinstance(value, (int, float, str)): return decimal.Decimal(value).quantize(decimal.Decimal("0.1") ** 8)
        else: raise connection.errors.InvalidError("Invalid type(%s) given in CCXTConnection.makeDecimal" % (type(value),))

    # ------------------------------------------------------------------------------------------------------------------
    # Asyncio control

    @staticmethod
    def fetchConcurrently(*coros):
        """
        <static method CCXTConnection.fetchConcurrently>
        Fetch request results concurrently. Responses are returned as list of result of each tasks.
        :param coros: Coroutines. CCXTConnection coroutines are only recommended to put as arguments.
        :return: Fetched results as list.
        """
        async def helper():
            tasks = []
            for coro in coros: tasks.append(asyncio.create_task(coro))
            result = []
            for task in tasks: result.append(await task)
            return result
        return asyncio.get_event_loop().run_until_complete(helper())

    # ------------------------------------------------------------------------------------------------------------------
    # Fetching markets

    async def fetchMarket(self, exchangeName: str):
        """
        <async method CCXTConnection.fetchMarket>
        :return: Market information for given exchange.
        """
        result = await self.exchanges[exchangeName].load_markets()
        market = {}
        for currencyPairStr in result:
            quote, base = currencyPairStr.split("/")
            if base not in market: market[base] = {}
            if quote in market[base]: raise KeyError("Duplicated values in loaded market data")
            market[base][quote] = result[currencyPairStr]
        return market

    async def fetchMarkets(self, exchangeNames = ()):
        """
        <async method CCXTConnection.fetchMarkets>
        Fetch markets and currencies information for given exchange names.
        :param exchangeNames: If not given then this method will fetch all exchanges in this connection.
        """
        if not exchangeNames: exchangeNames = self.exchanges.keys()
        for exchangeName in exchangeNames: self.markets[exchangeName] = await self.fetchMarket(exchangeName)

    # ------------------------------------------------------------------------------------------------------------------
    # Fetching balances

    unnecessaryBalanceTags = ("free", "total", "used", "info")
    async def fetchBalance(self, exchangeName: str, removeZero: bool = False):
        """
        <async method CCXTConnection.fetchBalance>
        Fetch account balance for given exchange name.
        :param exchangeName: Exchange name.
        :param removeZero: If this is True then the method will remove unnecessary zero balances.
        :return: Account balance information for given exchange.
        """
        result = await self.exchanges[exchangeName].fetch_balance() # CCXT fetch
        for untag in CCXTConnection.unnecessaryBalanceTags: del result[untag] # Remove unnecessary tags
        for currency in (tuple(result.keys()) if removeZero else result):
            allZero = True
            for ftu in result[currency]:
                result[currency][ftu] = CCXTConnection.makeDecimal(result[currency][ftu])
                if result[currency][ftu] != 0: allZero = False
            if removeZero and allZero: del result[currency] # Remove empty balances
        return result

    async def fetchBalances(self, exchangeNames = ()):
        """
        <async method CCXTConnection.fetchBalances>
        Fetch account balances for given exchange names. Zero balance will be removed.
        :param exchangeNames: If not given then this method will fetch all exchanges in this connection.
        """
        if not exchangeNames: exchangeNames = self.exchanges.keys()
        for exchangeName in exchangeNames: self.balance[exchangeName] = await self.fetchBalance(exchangeName, True)

    # ------------------------------------------------------------------------------------------------------------------
    # Fetching price and order books

    unnecessaryOrderbookTags = ("datetime", "nonce", "timestamp")
    async def fetchOrderbook(self, exchangeName: str, base: str, quote: str, removeUnnecessaryTags: bool = True):
        """
        <async method CCXTConnection.fetchOrderbook>
        Fetch orderbook for given exchange name and market.
        :return: Orderbook information, formatted as {}
        """

        self.raiseIfNotSupported(exchangeName, base, quote)
        result = await self.exchanges[exchangeName].fetch_order_book("%s/%s" % (quote, base))
        if removeUnnecessaryTags:
            for untag in CCXTConnection.unnecessaryOrderbookTags: del result[untag]
        for ask_or_bid in ("asks", "bids"):
            old_data = result[ask_or_bid]
            result[ask_or_bid] = {}
            for price, amount in old_data:
                price = CCXTConnection.makeDecimal(price)
                amount = CCXTConnection.makeDecimal(amount)
                result[ask_or_bid][price] = amount
        return result

    async def fetchOrderbooks(self, targets: (list, tuple)):
        """
        <async method CCXTConnection.fetchOrderbooks>
        Fetch current order books for given (exchange, base, quote) tuples.
        :param targets: [(exchange, base, quote), ...]
        """
        tasks = {}
        for exchangeName, base, quote in targets:
            if exchangeName not in tasks: tasks[exchangeName] = {}
            if base not in tasks[exchangeName]: tasks[exchangeName][base] = {}
            if quote in tasks[exchangeName][base]: raise connection.errors.InvalidError("Duplicated markets")
            tasks[exchangeName][base][quote] = asyncio.create_task(self.fetchOrderbook(exchangeName, base, quote))
        result = {}
        for exchangeName in tasks:
            if exchangeName not in result: result[exchangeName] = {}
            for base in tasks[exchangeName]:
                if base not in result[exchangeName]: result[exchangeName][base] = {}
                for quote in tasks[exchangeName][base]:
                    result[exchangeName][base][quote] = await tasks[exchangeName][base][quote]
        return result

# ----------------------------------------------------------------------------------------------------------------------
# Functionality Testing

from pprint import pprint
import time

if __name__ == "__main__":

    ccxtcon = CCXTConnection.makeFromFile(Upbit = "upbit.authkey", Binance = "binance.authkey",
                                          Bithumb = "bithumb.authkey")

    #pprint(ccxtcon.markets["Upbit"])
    #pprint(ccxtcon.markets["Binance"])
    #pprint(ccxtcon.balance)

    #tm = TimeMeasure()
    #pprint(asyncio.get_event_loop().run_until_complete(ccxtcon.fetchOrderbooks([("Upbit", "KRW", "BTC")])))
    #print("%.2f seconds used" % tm.update())

    result1 = asyncio.get_event_loop().run_until_complete(
        ccxtcon.fetchOrderbooks([("Upbit", "KRW", "BTC"), ("Binance", "USDT", "ETH")]))
    pprint(result1)

    '''
    results = ccxtcon.fetchConcurrently(ccxtcon.fetchOrderbook("Upbit", "KRW", "BTC"),
                                        ccxtcon.fetchOrderbook("Binance", "USDT", "ETH"),
                                        ccxtcon.fetchBalance("Bithumb", True))
    pprint(results)
    '''