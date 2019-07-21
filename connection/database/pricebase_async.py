"""
<module AutoTrade.database.pricebase_async>
Abstract base of all async database connection.
"""

# ----------------------------------------------------------------------------------------------------------------------
# Libraries

# Standard libraries
from copy import deepcopy
from decimal import Decimal
from datetime import datetime, timedelta
from collections import namedtuple
import asyncio

# External libraries
import asyncpg, asyncpg.exceptions

# Custom libraries
from .base import AbstractPGDBConnectionClass
import connection.errors as cerr

# ----------------------------------------------------------------------------------------------------------------------
# Pricebase

class PriceBaseClass(AbstractPGDBConnectionClass):
    """
    <class PriceBaseClass> inherited from AbstractPGDBConnectionClass
    Base asynchronous database template to store price data(OHLCV and tick data).

    All query-related method should contain 'exchange', 'base', 'quote', 'minuteInterval'.
    Minute interval is positive number, -1 for exception of tick data.
    Those are decorated by validity checking method.
    """

    defaultDBname = "PriceBaseClass"
    baseMinuteIntervals = {timedelta(seconds = 60), } # Base minute intervals to handle
    tickInterval = timedelta()

    # ------------------------------------------------------------------------------------------------------------------
    # Constructors / Initializing

    def __init__(self, userName: str, password: str, DBname: str = defaultDBname,
                 host: str = AbstractPGDBConnectionClass.defaultHost,
                 port: int = AbstractPGDBConnectionClass.defaultPortNumber,
                 connectionName: str = None, additionalMarkets: dict = None, callLimits: dict = None):

        # Parent class initialization; Key is not given to parent class because it's handled by db connection
        super().__init__(userName, password, DBname, host = host, port = port,
                         connectionName = connectionName, callLimits = callLimits)

        # Migrate markets
        self.markets = {}
        if isinstance(additionalMarkets, dict):
            for exchange in additionalMarkets:
                self.markets[exchange] = {}
                for base in additionalMarkets[exchange]:
                    self.markets[exchange][base] = {}
                    for quote in additionalMarkets[exchange][base]:
                        self.markets[exchange][base][quote] = deepcopy(PriceBaseClass.baseMinuteIntervals)
                        for minuteInterval in additionalMarkets[exchange][base][quote]:
                            minuteInterval = PriceBaseClass.interval(minuteInterval)
                            self.markets[exchange][base][quote].add(minuteInterval)

    async def _init_async(self, userName: str, password: str, DBname: str = defaultDBname,
                              host: str = AbstractPGDBConnectionClass.defaultHost,
                              port: int = AbstractPGDBConnectionClass.defaultPortNumber):
        """
        <async method PriceBaseClass._init_async>
        Initialize for async things.
        """

        # Parent class initialization
        await super()._init_async(host, port, userName, password, DBname)
        del password

        # Refresh market tables.
        await self.refreshMarketTables()


    async def refreshMarketTables(self, additionalMarkets: dict = None):
        """
        <async method PriceBaseClass.refreshMarketTables>
        Refresh market tables from current database table names and additional markets information.
        :param additionalMarkets: Additional markets information to add.
        """

        # Add new markets from additionalMarkets to self.markets
        if additionalMarkets:
            for exchange in additionalMarkets:
                if exchange not in self.markets: self.markets[exchange] = {}
                for base in additionalMarkets[exchange]:
                    if base not in self.markets[exchange]: self.markets[exchange][base] = {}
                    for quote in additionalMarkets[exchange][base]:
                        if quote not in self.markets[exchange][base]: self.markets[exchange][base][quote] = set()
                        for interval in self.markets[exchange][base][quote]:
                            self.markets[exchange][base][quote].add(PriceBaseClass.interval(interval))

        # Fetch tables
        async with self.connection.transaction():

            # Fetch table names
            tableNames = [record[0] for record in await self.connection.fetch("""
                        SELECT table_name FROM information_schema.tables
                        WHERE table_schema = 'public' AND table_type = 'BASE TABLE';""")]

            # Add information to self.markets from fetched table names
            for tableName in tableNames:
                if tableName.startswith(
                        "PriceData_"):  # Syntax should be PriceData_<EXCHANGE>_<BASE>_<QUOTE>_<AGG>mins or _tick
                    _, exchange, base, quote, minuteInterval = [c.strip(" ") for c in tableName.split("_")]
                    if minuteInterval == "tick":
                        minuteInterval = PriceBaseClass.tickInterval
                    else:
                        minuteInterval = timedelta(minutes = int(minuteInterval.replace("mins", "")))
                    if exchange not in self.markets: self.markets[exchange] = {}
                    if base not in self.markets[exchange]: self.markets[exchange][base] = {}
                    if quote not in self.markets[exchange][base]:
                        self.markets[exchange][base][quote] = deepcopy(PriceBaseClass.baseMinuteIntervals)
                    self.markets[exchange][base][quote].add(minuteInterval)

        # Create tables with current self.markets
        async with self.connection.transaction():
            for exchange in self.markets:
                for base in self.markets[exchange]:
                    for quote in self.markets[exchange][base]:
                        for interval in self.markets[exchange][base][quote]:
                            tableName = self.tableName(exchange, base, quote, interval)
                            print("tableName = %s, interval = %s" % (tableName, interval))
                            interval = PriceBaseClass.interval(interval)
                            if interval != timedelta(minutes = 1):
                                print("yuck!")
                                continue
                            totalSeconds = round(interval.total_seconds())
                            if interval == PriceBaseClass.tickInterval: # Tick data table initialization
                                await self.execute("""
                                    CREATE TABLE IF NOT EXISTS {T} (
                                        timestamp TIMESTAMPTZ PRIMARY KEY,
                                        price NUMERIC(24, 8) NOT NULL,
                                        volume NUMERIC(24, 8) NOT NULL,
                                        CHECK(volume > 0)
                                    )""", (tableName,))
                            else: # OHLCV table initialization
                                await self.execute("""
                                    CREATE TABLE IF NOT EXISTS {T} (
                                        timestamp   TIMESTAMPTZ PRIMARY KEY,
                                        open        NUMERIC(24, 8) NOT NULL,
                                        high        NUMERIC(24, 8) NOT NULL,
                                        low         NUMERIC(24, 8) NOT NULL,
                                        close       NUMERIC(24, 8) NOT NULL,
                                        volume      NUMERIC(24, 8) NOT NULL,
                                        CHECK(volume > 0),
                                        CHECK(CAST(ROUND(EXTRACT(epoch from timestamp)) AS BIGINT) %% CAST(%d AS BIGINT) = CAST(0 AS BIGINT))
                                    )""" % (totalSeconds,), (tableName,))

    # ------------------------------------------------------------------------------------------------------------------
    # Termination

    def terminate(self):
        raise NotImplementedError

    async def _terminate_async(self):
        pass

    # ------------------------------------------------------------------------------------------------------------------
    # Helper functions - Finance related

    @staticmethod
    def interval(interval) -> timedelta:
        """
        <static method PriceBaseClass.interval>
        :param interval: Given interval which is wanted to convert. Given interval should be one of:
            - string "tick" (implies tick data)
            - int, float (0 implies tick data, otherwise implies OHLCV)
            - timedelta (0 implies tick data, otherwise implies OHLCV)
        :return: Generated interval. Raised when the given interval is invalid.
        """
        if isinstance(interval, timedelta):
            if interval.seconds % 60 == 0 and interval.microseconds == 0: return interval
            else: raise cerr.InvalidError("Invalid size of interval; %s given." % (interval,))
        elif interval == "tick": return PriceBaseClass.tickInterval
        elif isinstance(interval, (int, float)):
            if interval >= 0 and interval % 60 == 0: return timedelta(seconds = interval)
            else: raise cerr.InvalidValueError("Invalid size of interval; %s seconds given." % (interval,))
        else: raise cerr.InvalidTypeError("Invalid type of interval; %s given." % (type(interval),))

    # ------------------------------------------------------------------------------------------------------------------
    # Helper functions - SQL related

    @staticmethod
    def tableName(exchange: str, base: str, quote: str, interval: timedelta) -> str:
        """
        <static method PriceBaseSync.tableName>
        Note that this method doesn't check if given market is supported or not. (That's why this method is static.)
        :return: Table name generated from given exchange, base, quote, and minute interval.
        """
        interval = PriceBaseClass.interval(interval) # Automatically qualified given interval
        if interval == PriceBaseClass.tickInterval: return "PriceData_%s_%s_%s_tick" % (exchange, base, quote)
        else:
            totalSecs = interval.seconds + interval.days * 60 * 60 * 24
            return "PriceData_%s_%s_%s_%dmins" % (exchange, base, quote, totalSecs // 60)

    def availableIntervals(self, exchange: str, base: str, quote: str) -> set:
        """
        <method PriceBaseSync.availableIntervals>
        :return: Set of available intervals from currently supporting markets. Empty set for non-existing market.
        """
        try: return self.markets[exchange][base][quote]
        except KeyError: return set()

    def raiseIfNotSupported(self, exchange: str, base: str, quote: str, minuteInterval: timedelta):
        """
        <method PriceBaseSync.raiseIfNotSupported>
        Raise error if given minute interval is not supported for given market.
        """
        if minuteInterval not in self.availableIntervals(exchange, base, quote):
            raise cerr.MarketNotSupported(exchange, base, quote)

    # ------------------------------------------------------------------------------------------------------------------
    # Fetch

    async def select(self, exchange: str, base: str, quote: str, interval: timedelta,
                     beginTime: datetime, endTime: datetime = None,
                     limit: int = None, timeout: float = None) -> dict:
        """
        <async method PriceBaseClass.select>
        Select all price data between given timestamps.
        :return: Post-processed fetched data in dict form. If no data was available, return empty dict.
        """

        # Validation
        if endTime is None: endTime = datetime.now() # Automatically converted if not given
        if beginTime > endTime: raise cerr.InvalidValueError("Given time begin point(%s) is later than end point(%s)" %
                                                             (beginTime, endTime))
        elif limit is not None:
            if not isinstance(limit, int): raise cerr.InvalidTypeError("Given limit has invalid type(%s)" % (type(limit),))
            elif limit <= 0: raise cerr.InvalidValueError("Non-positive limit(%d) given" % (limit,))
        self.raiseIfNotSupported(exchange, base, quote, interval)
        tableName = self.tableName(exchange, base, quote, interval)

        # Execute query; Expected result = [row, row, ...] or None
        data = await self.execute("SELECT * FROM {T} WHERE timestamp BETWEEN $1 AND $2 " +
                                  ("LIMIT %d" % (limit,) if limit else ""),
                                  (tableName,), beginTime, endTime, timeout = timeout, fetch = True)

        # Post-process data
        result = {}
        if data: # If data was successfully returned
            if interval == PriceBaseClass.tickInterval: # [(timestamp, price, volume), ...]
                datatype = namedtuple("TICK", ["price", "volume"])
                for row in data:
                    timestamp, price, volume = row[0], row[1], row[2]
                    result[timestamp] = datatype(price, volume) # {timestamp: (price, volume), ...}
            else: # [(timestamp, O, H, L, C, V), ...]
                datatype = namedtuple("OHLCV", ["open", "high", "low", "close", "volume"])
                for row in data:
                    timestamp, ohlcv = row[0], row[1:]
                    result[timestamp] = datatype(*ohlcv) # {timestamp: (O, H, L, C, V), ...}
        return result

    # ------------------------------------------------------------------------------------------------------------------
    # Insert and update

    async def append(self, exchange: str, base: str, quote: str, interval: timedelta, timestamp: datetime,
                     open: Decimal = None, high: Decimal = None, low: Decimal = None, close: Decimal = None,
                     volume: Decimal = None, price: Decimal = None):
        """
        <async method PriceBaseClass.append>
        Append new price data to table.
        """

        # Validation

# ----------------------------------------------------------------------------------------------------------------------
# Extra

pass

# ----------------------------------------------------------------------------------------------------------------------
# Async initializer

async def PriceBase(userName: str = None, password: str = None, DBname: str = None,
                    host: str = AbstractPGDBConnectionClass.defaultHost,
                    port: int = AbstractPGDBConnectionClass.defaultPortNumber,
                    fileName: str = None):

    if fileName is not None:
        with open(fileName) as file:
            username, password, host, port, dbname = [c.strip(' ') for c in file.read().split("\n")]
            if not host: host = "localhost"
            if not port or not port.isdigit():
                port = PriceBaseClass.defaultPortNumber
            else:
                port = int(port)
            if not dbname: dbname = PriceBaseClass.defaultDBname
        DB = PriceBaseClass(username, password, dbname, host, port)
        await DB._init_async(username, password, dbname, host, port)
        return DB
    else:
        raise NotImplementedError

# ----------------------------------------------------------------------------------------------------------------------
# Testing

if __name__ == "__main__":

    async def test():
        pricebase = PriceBaseClass()

"""
TODO: Aggregation
--- SQL Query Below ---

WITH 

temptable as (SELECT *
FROM "PriceData_Bitstamp_USD_BTC_1mins" WHERE 
'2017-06-21 10:20:00+00' <= timestamp AND 
timestamp < '2017-06-21 10:25:00+00'),

openprice as (SELECT "open" FROM temptable ORDER BY "timestamp" ASC LIMIT 1),
closeprice as (SELECT "close" FROM temptable ORDER BY "timestamp" DESC LIMIT 1)

SELECT 
min(temptable.timestamp) as "timestamp", 
(SELECT sum("open") FROM openprice) as "open", 
max("high") as "high", min("low") as "low", 
(SELECT sum("close") FROM closeprice) as "close", 
sum("volume") as "volume"
FROM temptable;
"""