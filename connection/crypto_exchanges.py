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
from decimal import Decimal

# External libraries
import ccxt.async_support as ccxt

# Custom libraries
from connection.base import AbstractConnection
import connection.errors as cerr

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

        # Save market by order ID. {(exchangeName, orderID): ()}
        self.marketByOrderID = {}

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
        Close all exchange sessions asynchronously. CCXT.async_support requires to close at the end of the program.
        """
        tasks = []
        for exchangeName in self.exchanges:
            tasks.append(asyncio.create_task(self.exchanges[exchangeName].close()))
        for task in tasks: await task

    def terminate(self):
        try:
            asyncio.get_event_loop().run_until_complete(self.closeExchangeSessions()) # Close exchange sessions
        except:
            pass

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
        if not self.isSupported(exchange, base, quote):
            raise cerr.MarketNotSupported(exchange, base, quote)

    @staticmethod
    def makeDecimal(value):
        if value is None: return Decimal(0)
        elif isinstance(value, Decimal): return value.quantize(Decimal("0.1") ** 8)
        elif isinstance(value, (int, float, str)): return Decimal(value).quantize(Decimal("0.1") ** 8)
        else: raise cerr.InvalidError("Invalid type(%s) given in CCXTConnection.makeDecimal" % (type(value),))

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
    #@AbstractConnection._makeCallSync
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
    # Fetching price and orderbooks

    reversedAsksBids = {"asks": "bids", "bids": "asks"}
    @staticmethod
    def reversedOrderbook(orderbook):
        """
        <method CCXTConnection.reversedOrderbook>
        Create and return reversed orderbook.
        """
        result = {"asks": {}, "bids": {}, "reversed": not orderbook["reversed"]}
        for ask_or_bid in ("asks", "bids"):
            for price in orderbook[ask_or_bid]:
                amount = orderbook[ask_or_bid][price]
                result[CCXTConnection.reversedAsksBids[ask_or_bid]][price ** -1] = price * amount
        return result

    unnecessaryOrderbookTags = ("datetime", "nonce", "timestamp")
    async def fetchOrderbook(self, exchangeName: str, base: str, quote: str,
                             removeUnnecessaryTags: bool = True, processReversed: bool = False):
        """
        <async method CCXTConnection.fetchOrderbook>
        Fetch orderbook for given exchange name and market.
        :return: Orderbook information, formatted as
            {"bids": {price: amount, ..}, "asks": {price: amount, ..}, "reversed": T/F}
        """

        # Process if need to support reversed orderbook
        if processReversed and not self.isSupported(exchangeName, base, quote) and self.isSupported(exchangeName, quote, base):
            return CCXTConnection.reversedOrderbook(
                await self.fetchOrderbook(exchangeName, quote, base, removeUnnecessaryTags = removeUnnecessaryTags))

        # Main processing
        self.raiseIfNotSupported(exchangeName, base, quote)
        result = await self.exchanges[exchangeName].fetch_order_book("%s/%s" % (quote, base))
        if removeUnnecessaryTags:
            for unnecessaryTag in CCXTConnection.unnecessaryOrderbookTags: del result[unnecessaryTag]
        for ask_or_bid in ("asks", "bids"):
            old_data = result[ask_or_bid]
            result[ask_or_bid] = {}
            for price, amount in old_data:
                price = CCXTConnection.makeDecimal(price)
                amount = CCXTConnection.makeDecimal(amount)
                result[ask_or_bid][price] = amount
        result["reversed"] = False
        return result

    async def fetchOrderbooks(self, targets: (list, tuple), processReversed: bool = False):
        """
        <async method CCXTConnection.fetchOrderbooks>
        Fetch current order books for given (exchange, base, quote) tuples.
        :param targets: [(exchange, base, quote), ...]
        :param processReversed: If true then try to gather reversed orderbooks if possible.
        """

        # Create tasks
        tasks = {}
        for exchangeName, base, quote in targets:
            if exchangeName not in tasks: tasks[exchangeName] = {}
            if base not in tasks[exchangeName]: tasks[exchangeName][base] = {}
            if quote in tasks[exchangeName][base]: raise cerr.InvalidError("Duplicated markets")
            tasks[exchangeName][base][quote] = asyncio.create_task(
                self.fetchOrderbook(exchangeName, base, quote, processReversed = processReversed))

        # Await tasks
        result = {}
        for exchangeName in tasks:
            if exchangeName not in result: result[exchangeName] = {}
            for base in tasks[exchangeName]:
                if base not in result[exchangeName]: result[exchangeName][base] = {}
                for quote in tasks[exchangeName][base]:
                    result[exchangeName][base][quote] = await tasks[exchangeName][base][quote]
        return result

    # ------------------------------------------------------------------------------------------------------------------
    # Order

    async def fetchOpenOrders(self, exchangeName: str, base: str = "", quote: str = "", processReversed: bool = False):
        """
        <async method CCXTConnection.fetchOpenOrders>
        :return: All fetched open orders for given market.
        """
        if base and quote:
            if self.isSupported(exchangeName, base, quote): # Specify given symbol is supported
                return await self.exchanges[exchangeName].fetchOpenOrders(symbol="%s/%s" % (quote, base))
            elif self.isSupported(exchangeName, quote, base) and processReversed: # Fetch open orders in reversed market
                return await self.fetchOpenOrders(exchangeName, quote, base, processReversed = True)
            else: raise cerr.MarketNotSupported(exchangeName, base, quote) # Given market not found
        else: return await self.exchanges[exchangeName].fetchOpenOrders()

    async def createOrder(self, exchangeName: str, base: str, quote: str,
                          price: (int, float, Decimal), amount: (int, float, Decimal), buy: bool = True):
        """
        <async method CCXTConnection.order>
        Create order based on given exchange, base, quote.
        If base and quote are reversed, then automatically reverse it and process all related values(amount, price, etc)
        Price is described by [1 quote = $price base], and the unit of amount is quote.
        :return: Order ID.
        """

        # Non positive price error
        if price <= 0: raise cerr.InvalidError("Cannot create orders with non-positive(%s) price" % (price,))

        # If given market is not supported then try to find reversed pair
        if not self.isSupported(exchangeName, base, quote):
            if self.isSupported(exchangeName, quote, base):
                return await self.createOrder(exchangeName, quote, base, price ** -1, price*amount, buy = not buy)
            else: raise cerr.MarketNotSupported(exchangeName, base, quote)

        # Create order and return order ID
        result = await self.exchanges[exchangeName].createOrder(
            "%s/%s" % (quote, base), "limit", "buy" if buy else "sell", amount, price)
        orderID = result["id"]
        self.marketByOrderID[exchangeName, orderID] = (base, quote)
        return orderID

    async def cancelOrder(self, exchangeName: str, orderID: str, explicitBase: str = None, explicitQuote: str = None):
        """
        <method CCXTConnection.cancelOrder>
        Cancel order with given exchange name and order ID.
        :return: Exchange response
        """

        if explicitBase and explicitQuote: # If the pair is explicitly provided then use it
            return await self.exchanges[exchangeName].cancelOrder(orderID, "%s/%s" % (explicitQuote, explicitBase))
        elif (exchangeName, orderID) in self.marketByOrderID: # If the pair by orderID is available then use it
            base, quote = self.marketByOrderID[exchangeName, orderID]
            return await self.exchanges[exchangeName].cancelOrder(orderID, "%s/%s" % (quote, base))
        else: # Otherwise just cancel with only orderID
            return await self.exchanges[exchangeName].cancelOrder(orderID)

# ----------------------------------------------------------------------------------------------------------------------
# Functionality Testing

from pprint import pprint

if __name__ == "__main__":

    ccxtcon = CCXTConnection.makeFromFile(
        Upbit = "upbit.authkey")

    #pprint(ccxtcon.markets["Upbit"])
    #pprint(ccxtcon.markets["Binance"])
    #pprint(ccxtcon.balance)

    #tm = TimeMeasure()
    #pprint(asyncio.get_event_loop().run_until_complete(ccxtcon.fetchOrderbooks([("Upbit", "KRW", "BTC")])))
    #print("%.2f seconds used" % tm.update())

    result1 = asyncio.get_event_loop().run_until_complete(
        ccxtcon.fetchOrderbook("Upbit", "KRW", "BTC"))
    pprint(result1)