"""
<module AutoTrade.database.pricebase>
Abstract base of all database connection.
"""

# ----------------------------------------------------------------------------------------------------------------------
# Libraries

# Standard libraries
from copy import deepcopy
from decimal import Decimal
from datetime import datetime

# External libraries
import pytz # Timezone covering
import psycopg2 # PostgreSQL binder

# Custom libraries

# ----------------------------------------------------------------------------------------------------------------------
# Pricebase

class PriceBase:
    """
    <class PriceBase>
    Base database object to store price data.
    """

    defaultPortNumber = 5432 # Default port number for PostgreSQL
    baseMinuteIntervals = {1,} # Base minute intervals to handle

    # ------------------------------------------------------------------------------------------------------------------
    # Constructors

    @staticmethod
    def openFromFile(filepath):
        """
        <method PriceBase.openFromFile> (static method)
        Create PriceBase object from given file. That file should have syntax with
            <username>
            <password>
            <host> (if host is blank then set to 'localhost')
            <port> (if port is blank then set to 5432)
        :param filepath: File path to open.
        :return: PriceBase object
        """
        with open(filepath) as file: username, password, host, port = file.read().split("\n")
        if not host: host = "localhost"
        if not port or not port.isdigit(): port = PriceBase.defaultPortNumber
        else: port = int(port)
        return PriceBase(username, password, host = host, port = port)

    def __init__(self, username: str, password: str, mainDBname: str = "PriceBase",
                 host: str = "localhost", port: int = defaultPortNumber,
                 additionalMarkets: dict = None):

        # Create connections
        self.connection = psycopg2.connect(user = username, password = password, dbname = mainDBname,
                                           host = host, port = port)
        del username, password

        # Migrate markets
        self.markets = {}
        if type(additionalMarkets) is dict:
            for exchange in additionalMarkets:
                self.markets[exchange] = {}
                for base in additionalMarkets[exchange]:
                    self.markets[exchange][base] = {}
                    for quote in additionalMarkets[exchange][base]:
                        self.markets[exchange][base][quote] = deepcopy(PriceBase.baseMinuteIntervals)
                        for minuteInterval in additionalMarkets[exchange][base][quote]:
                            self.markets[exchange][base][quote].add(minuteInterval)

        # Search already existing markets and add in self.markets
        with self.connection.cursor() as cursor:
            cursor.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_type = 'BASE TABLE';""")
            for result in cursor:
                tablename = result[0]
                if tablename.startswith("PriceData_"): # Syntax should be PRICEDATA_EXCHANGE_BASE_QUOTE_aggregate(minute)
                    _, exchange, base, quote, minuteInterval = [c for c in tablename.split(" ")]
                    minuteInterval = int(minuteInterval.replace("mins", ""))
                    if exchange not in self.markets: self.markets[exchange] = {}
                    if base not in self.markets[exchange]: self.markets[exchange][base] = {}
                    if quote not in self.markets[exchange]: self.markets[exchange][base][quote] = deepcopy(PriceBase.baseMinuteIntervals)
                    self.markets[exchange][base][quote].add(minuteInterval)

        # Create tables and commit
        self.addMarketTables()
        self.connection.commit()

    # ------------------------------------------------------------------------------------------------------------------
    # Support with-clause

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.close()

    # ------------------------------------------------------------------------------------------------------------------
    # Helper

    @staticmethod
    def tableName(exchange: str, base: str, quote: str, minuteInterval: int) -> str:
        """
        <method PriceBase.tableName> (static method)
        :return: Table name generated from given exchange, base, quote, and minute interval.
        """
        return "PriceData_%s_%s_%s_%dmins" % (exchange.upper(), base.upper(), quote.upper(), minuteInterval)

    def availableIntervals(self, exchange: str, base: str, quote: str) -> set:
        """
        <method PriceBase.availableIntervals>
        :return: Set of available intervals from currently supporting markets. Empty set for non-existing market.
        """
        try: return self.markets[exchange][base][quote]
        except KeyError: return set()

    def addMarketTables(self):
        """
        <method PriceBase.addMarketTables>
        Add database tables for currently supporting markets.
        """
        with self.connection.cursor() as cursor:
            for exchange in self.markets:
                for base in self.markets[exchange]:
                    for quote in self.markets[exchange][base]:
                        for minuteInterval in self.markets[exchange][base][quote]:
                            cursor.execute("""
                                CREATE TABLE IF NOT EXISTS %s (
                                timestamp TIMESTAMPTZ PRIMARY KEY,
                                open NUMERIC(24, 8) NOT NULL,
                                high NUMERIC(24, 8) NOT NULL,
                                low NUMERIC(24, 8) NOT NULL,
                                close NUMERIC(24, 8) NOT NULL,
                                volume NUMERIC(24, 8) NOT NULL,
                                CHECK(volume > 0), 
                                CHECK(timestamp <= NOW())
                            );""", PriceBase.tableName(exchange, base, quote, minuteInterval))

    # ------------------------------------------------------------------------------------------------------------------
    # Search data

    def getSingle(self, exchange: str, base: str, quote: str, minuteInterval: int, timestamp: datetime):
        """
        <method PriceBase.getSingle>
        :return: OHLCV with given market and timestamp. If no data found, return None instead.
        """

        assert minuteInterval in self.availableIntervals(exchange, base, quote) # Data table should be available
        assert round(timestamp.timestamp()) % (minuteInterval * 60) == 0 # Timestamp should be divisible by interval
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""SELECT "open", "high", "low", "close", "volume" FROM %s WHERE "timestamp" = %s;""",
                               (self.tableName(exchange, base, quote, minuteInterval), timestamp))
                result = cursor.fetchall()
                assert len(result) < 2
                return result[0][1:6] if result else None
        except psycopg2.Error as err:
            print("Error occured: <%s>" % (str(err).replace("\n", " / ")))
            return None

    def getBetweenTimestamps(self, exchange: str, base: str, quote: str, minuteInterval: int,
                             startTime: datetime, endTime = datetime):
        """
        <method PriceBase.getBetweenTimestamps>
        :return: Dict of OHLCVs with given market and timestamp. {timestamp: (O, H, L, C, V), ...}
        """

        assert minuteInterval in self.availableIntervals(exchange, base, quote) # Data table should be available
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""SELECT * FROM %s WHERE timestamp BETWEEN %s AND %s;""",
                               (PriceBase.tableName(exchange, base, quote, minuteInterval), startTime, endTime))
                result = {}
                for timestamp, O, H, L, C, V in cursor: result[timestamp] = (O, H, L, C, V)
                return result
        except psycopg2.Error as err:
            print("Error occured: <%s>" % (str(err).replace("\n", " / ")))
            return {}

    # ------------------------------------------------------------------------------------------------------------------
    # Insert or Update data

    def addSingleOHLCV(self, exchange: str, base: str, quote: str, minuteInterval: int,
                       timestamp: datetime, O: Decimal, H: Decimal, L: Decimal, C: Decimal, V: Decimal) -> bool:
        """
        <method PriceBase.addSingleOHLCV>
        Insert or update single (timestamp, O, H, L, C, V) into database.
        :return: If the operation was successful
        """

        assert minuteInterval in self.availableIntervals(exchange, base, quote) # Data table should be available
        try: # First try with insertion
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO %s ("timestamp", "open", "high", "low", "close", "volume")
                    VALUES (%s, %s, %s, %s, %s, %s);
                """, (PriceBase.tableName(exchange, base, quote, minuteInterval), timestamp, O, H, L, C, V))
            return True
        except psycopg2.IntegrityError: # Second try with update
            try:
                with self.connection.cursor() as cursor:
                    cursor.execute("""
                        UPDATE %s 
                        SET open = %s, high = %s, low = %s, close = %s, volume = %s
                        WHERE timestamp = %s;
                    """, (PriceBase.tableName(exchange, base, quote, minuteInterval), O, H, L, C, V, timestamp))
                return True
            except psycopg2.Error: pass
        except psycopg2.Error: pass
        return False
