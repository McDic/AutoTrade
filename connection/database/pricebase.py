"""
<module AutoTrade.database.pricebase>
Abstract base of all database connection.
"""

# ----------------------------------------------------------------------------------------------------------------------
# Libraries

# Standard libraries
from copy import deepcopy
from decimal import Decimal
from datetime import datetime, timedelta
import atexit

# External libraries
import psycopg2
from psycopg2.sql import SQL, Identifier, Literal # Advanced querying

# Custom libraries
from connection.base import AbstractConnection
import connection.errors as cerr

# ----------------------------------------------------------------------------------------------------------------------
# Pricebase

class PriceBase(AbstractConnection):
    """
    <class PriceBase>
    Base database object to store price data(OHLCV and tick data).

    Keep in mind that this object don't support asynchronous querying yet.
    Multithreading is possible, but since psycopg2 only supports revoking transaction at connection-object level, I don't recommend it.

    All query-related method should contain 'exchange', 'base', 'quote', 'minuteInterval' as first 4 arguments.
    Those are decorated by validity checking method.
    """

    defaultDBname = "PriceBase"
    defaultPortNumber = 5432 # Default port number for PostgreSQL
    baseMinuteIntervals = {1,} # Base minute intervals to handle

    # ------------------------------------------------------------------------------------------------------------------
    # Constructors

    def __init__(self, username: str, password: str, dbname: str = defaultDBname,
                 host: str = "localhost", port: int = defaultPortNumber,
                 additionalMarkets: dict = None, callLimits: dict = None):

        # Parent class initialization; Key is not given to parent class because it's handled by db connection
        super().__init__(connectionName = "PostgreSQL %s/%s Connection" % (dbname, username), callLimits = callLimits)

        # Create DB connection
        self.connection = psycopg2.connect(user = username, password = password, dbname = dbname,
                                           host = host, port = port)
        del username, password

        # Migrate markets
        self.markets = {}
        if isinstance(additionalMarkets, dict):
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
                if tablename.startswith("PriceData_"): # Syntax should be PRICEDATA_(EXCHANGE)_(BASE)_(QUOTE)_((aggregate)mins | tick)
                    _, exchange, base, quote, minuteInterval = [c.strip(" ") for c in tablename.split("_")]
                    if minuteInterval == "tick": minuteInterval = 0
                    else: minuteInterval = int(minuteInterval.replace("mins", ""))
                    if minuteInterval < 0: raise cerr.InvalidError("Negative minute interval(%d) found" % (minuteInterval,))
                    if exchange not in self.markets: self.markets[exchange] = {}
                    if base not in self.markets[exchange]: self.markets[exchange][base] = {}
                    if quote not in self.markets[exchange][base]: self.markets[exchange][base][quote] = deepcopy(PriceBase.baseMinuteIntervals)
                    self.markets[exchange][base][quote].add(minuteInterval)

        # Create tables
        self.addMarketTables()

    # ------------------------------------------------------------------------------------------------------------------
    # Termination

    def terminate(self):
        super().terminate()
        if not self.connection.closed: self.connection.close()

    # ------------------------------------------------------------------------------------------------------------------
    # Helper

    @staticmethod
    def tableName(exchange: str, base: str, quote: str, minuteInterval: (int, timedelta)) -> str:
        """
        <method PriceBase.tableName> (static method)
        This method doesn't check if given market is supported or not. So be careful with usage.
        If given minuteInterval is zero, then tick data table name will be returned.
        :return: Table name generated from given exchange, base, quote, and minute interval.
        """
        if minuteInterval == 0: return "PriceData_%s_%s_%s_tick" % (exchange, base, quote)
        elif isinstance(minuteInterval, int) and minuteInterval > 0:
            return "PriceData_%s_%s_%s_%dmins" % (exchange, base, quote, minuteInterval)
        elif isinstance(minuteInterval, timedelta) and minuteInterval.total_seconds() % 60 == 0:
            return "PriceData_%s_%s_%s_%dmins" % (exchange, base, quote, minuteInterval.total_seconds() // 60)
        else: raise cerr.InvalidError("Invalid minute interval(%s) given" % (minuteInterval,))

    def availableIntervals(self, exchange: str, base: str, quote: str) -> set:
        """
        <method PriceBase.availableIntervals>
        :return: Set of available intervals from currently supporting markets. Empty set for non-existing market.
        """
        try: return self.markets[exchange][base][quote]
        except KeyError: return set()

    def raiseIfNotSupported(self, exchange, base, quote, minuteInterval):
        """
        <method PriceBase.raiseIfNotSupported>
        Raise error if given minute interval is not supported for given market.
        """
        if minuteInterval not in self.availableIntervals(exchange, base, quote):
            raise cerr.MarketNotSupported(exchange, base, quote)

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

                            # Tick
                            if minuteInterval == 0: cursor.execute(SQL("""
                                CREATE TABLE IF NOT EXISTS {} (
                                timestamp TIMESTAMPTZ PRIMARY KEY,
                                price NUMERIC(24, 8) NOT NULL,
                                volume NUMERIC(24, 8) NOT NULL,
                                CHECK(volume > 0),
                                CHECK(timestamp <= NOW())
                            );""").format(Identifier(PriceBase.tableName(exchange, base, quote, 0))))

                            # OHLCV
                            else: cursor.execute(SQL("""
                                CREATE TABLE IF NOT EXISTS {0} (
                                timestamp TIMESTAMPTZ PRIMARY KEY,
                                open NUMERIC(24, 8) NOT NULL,
                                high NUMERIC(24, 8) NOT NULL,
                                low NUMERIC(24, 8) NOT NULL,
                                close NUMERIC(24, 8) NOT NULL,
                                volume NUMERIC(24, 8) NOT NULL,
                                CHECK(volume > 0), 
                                CHECK(timestamp <= NOW()),
                                CHECK(CAST(ROUND(EXTRACT(epoch from timestamp)) AS BIGINT) % {1} == 0)
                            );""").format(Identifier(PriceBase.tableName(exchange, base, quote, minuteInterval)),
                                          Literal(60 * minuteInterval)))
        self.connection.commit()

    # ------------------------------------------------------------------------------------------------------------------
    # Grant privileges

    def grant(self, privilege: str, username: str = "PUBLIC", give: bool = True):

        # Validity checking
        privilege = privilege.upper()
        if privilege not in ("SELECT", "INSERT", "UPDATE", "DELETE", "RULE", "ALL"):
            raise cerr.InvalidError("Given privilege(%s) is invalid" % (privilege,))

        try: # Try grant query
            with self.connection.cursor() as cursor:
                for exchange in self.markets:
                    for base in self.markets[exchange]:
                        for quote in self.markets[exchange][base]:
                            for minuteInterval in self.markets[exchange][base][quote]:
                                IDtuple = (psycopg2.sql.Identifier(self.tableName(exchange, base, quote, minuteInterval)),
                                           psycopg2.sql.Identifier(username))
                                if give: cursor.execute(SQL("GRANT %s ON {0} TO {1}" % (privilege,)).format(*IDtuple))
                                else: cursor.execute(SQL("REVOKE %s ON {0} FROM {1}" % (privilege,)).format(*IDtuple))
            self.connection.commit()
        except psycopg2.Error as err: # If error occurred then cancel everything
            print("Error (%s) occured while doing grant query <%s>" % (err, str(cursor.query)))
            self.connection.rollback()

    # ------------------------------------------------------------------------------------------------------------------
    # Search data

    def getSingleOHLCV(self, exchange: str, base: str, quote: str, minuteInterval: (int, timedelta), timestamp: datetime):
        """
        <method PriceBase.getSingleOHLCV>
        :return: OHLCV with given market and timestamp. If no data found, return None instead.
        """

        # Validity checking
        self.raiseIfNotSupported(exchange, base, quote, minuteInterval) # Data table should be available
        if isinstance(minuteInterval, timedelta):
            if minuteInterval.total_seconds() % 60 != 0:
                raise cerr.InvalidError("Invalid minute interval(%s) given;" % (minuteInterval,))
            else: minuteInterval = minuteInterval.total_seconds() // 60
        if minuteInterval <= 0: raise cerr.InvalidError("Invalid minute interval(%s) given, less or equal than zero." % (minuteInterval,))
        elif timestamp.timestamp() % (minuteInterval * 60) != 0: # Timestamp should be divisible by minute interval
            raise cerr.InvalidError("Given timestamp(%s) is not fit to given interval(%d minutes)" % (timestamp, minuteInterval))

        try: # Try selection
            with self.connection.cursor() as cursor:
                cursor.execute(SQL('SELECT "open", "high", "low", "close", "volume" FROM {} WHERE "timestamp" = %s;').format(
                    Identifier(self.tableName(exchange, base, quote, minuteInterval))), (timestamp,))
                result = cursor.fetchall()
                self.connection.commit()
                assert len(result) < 2 # Result should be unique
                return result[0][1:6] if result else None
        except psycopg2.Error as err:
            self.connection.rollback()
            print("Error occured while getting single OHLCV: <%s> // query: <%s>" % (err, cursor.query))
            return None

    def getMultiOHLCV(self, exchange: str, base: str, quote: str, minuteInterval: int,
                      startTime: datetime, endTime = datetime):
        """
        <method PriceBase.getBetweenTimestamps>
        :return: Dict of OHLCVs with given market and timestamp. {timestamp: (O, H, L, C, V), ...}
        """

        # Validity checking
        self.raiseIfNotSupported(exchange, base, quote, minuteInterval) # Data table should be available

        # Try selection
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(SQL('SELECT * FROM {} WHERE timestamp BETWEEN %s AND %s ORDER BY timestamp ASC;').format(
                    Identifier(PriceBase.tableName(exchange, base, quote, minuteInterval))), (startTime, endTime))
                self.connection.commit()
                result = {timestamp: (O, H, L, C, V) for (timestamp, O, H, L, C, V) in cursor}
                return result
        except psycopg2.Error as err:
            self.connection.rollback()
            print("Error occured in getBetweenTimestamps: <%s> // query: <%s>" % (err, cursor.query))
            return {}

    # ------------------------------------------------------------------------------------------------------------------
    # Insert or Update OHLCV data

    def addSingleOHLCV(self, exchange: str, base: str, quote: str, minuteInterval: int, timestamp: datetime,
                       O: Decimal, H: Decimal, L: Decimal, C: Decimal, V: Decimal,
                       override: bool = True, showDetailedProgress: bool = False) -> bool:
        """
        <method PriceBase.addSingleOHLCV>
        Insert or update single (timestamp, O, H, L, C, V) into database.
        :return: If the operation was successful
        """

        # Validity checking
        self.raiseIfNotSupported(exchange, base, quote, minuteInterval) # Data table should be available
        if timestamp.timestamp() % (minuteInterval * 60) != 0: # Timestamp should be divisible by minute interval
            raise cerr.InvalidError("Given timestamp(%s) is not fit to given interval(%d minutes)" % (timestamp, minuteInterval))

        # Try insertion
        tableName = self.tableName(exchange, base, quote, minuteInterval)
        try:
            with self.connection.cursor() as cursor:
                if override:
                    cursor.execute(SQL("""
                        INSERT INTO {} ("timestamp", "open", "high", "low", "close", "volume")
                        VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT ("timestamp")
                        DO UPDATE SET ("open", "high", "low", "close", "volume") = 
                        (EXCLUDED."open", EXCLUDED."high", EXCLUDED."low", EXCLUDED."close", EXCLUDED."volume");
                    """).format(Identifier(tableName)), (timestamp, O, H, L, C, V))
                else:
                    cursor.execute(SQL("""
                        INSERT INTO {} ("timestamp", "open", "high", "low", "close", "volume")
                        VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;
                    """).format(Identifier(tableName)), (timestamp, O, H, L, C, V))
                self.connection.commit()
            return True
        except psycopg2.Error as err: # If error occured then rollback
            if showDetailedProgress:
                print("psycopg2.Error <%s> raised while inserting single row in PriceBase.addSingleOHLCV (query = %s)" %
                      (err, str(cursor.query)))
            self.connection.rollback()
            return False

    def fastCopyOHLCV(self, exchange: str, base: str, quote: str, minuteInterval: int, filename: str,
                      separate: str = ",", tempTableName: str = "PriceDataTemp",
                      override: bool = True, showDetailedProgress: bool = True) -> bool:
        """
        <method PriceBase.fastCopyOHLCV>
        Fast bulk insertion using COPY query with given file. The given file should matches like csv, described below:
            <timestamp> <sep> <open> <sep> <high> <sep> <low> <sep> <close> <sep> <volume> [\n ... (repeat)]
        :return: If the query was successful.
        """

        # Validity checking
        self.raiseIfNotSupported(exchange, base, quote, minuteInterval)  # Data table should be available

        # Try copy
        tableName = self.tableName(exchange, base, quote, minuteInterval)
        with self.connection.cursor() as cursor, open(filename) as datafile:
            try:
                if showDetailedProgress:
                    print("Fast copying %s into (%s, %s, %s, %d)" % (filename, exchange, base, quote, minuteInterval))
                # 1. Create temporary table
                cursor.execute(SQL("CREATE TABLE IF NOT EXISTS {0} (LIKE {1});").format(Identifier(tempTableName), Identifier(tableName)))
                if showDetailedProgress: print("Temp table created(or already exist)")
                # 2. Delete all rows from temp table using truncate
                cursor.execute(SQL("TRUNCATE {}").format(Identifier(tempTableName)))
                if showDetailedProgress: print("Truncated temp table")
                # 3. Copy
                cursor.copy_expert(SQL("COPY {0} FROM STDIN (DELIMITER {1})").format(Identifier(tempTableName), Literal(separate)), datafile)
                if showDetailedProgress: print("Copied original data to temp table")
                # 4. Move all data from PriceDataTemp to target table
                if override:
                    cursor.execute(SQL("""
                        INSERT INTO {0} (SELECT * FROM {1}) ON CONFLICT ("timestamp")
                        DO UPDATE SET   ("open", "high", "low", "close", "volume") = 
                        (EXCLUDED."open", EXCLUDED."high", EXCLUDED."low", EXCLUDED."close", EXCLUDED."volume");
                    """).format(Identifier(tableName), Identifier(tempTableName)))
                    if showDetailedProgress: print("Moved data to actual table with conflict override action")
                else:
                    cursor.execute(SQL("INSERT INTO {0} (SELECT * FROM {1}) ON CONFLICT (\"timestamp\") DO NOTHING;").format(
                        Identifier(tableName), Identifier(tempTableName)))
                    if showDetailedProgress: print("Moved data to actual table without conflict override action")
                # 5. Drop
                cursor.execute(SQL("DROP TABLE {};").format(Identifier(tempTableName)))
                if showDetailedProgress: print("Dropped temp table")
            except psycopg2.Error as err:
                self.connection.rollback()
                if showDetailedProgress: print("psycopg2.Error <%s> occured while doing query <%s>" % (err, str(cursor.query)))
                return False
            else: self.connection.commit()
        return True

# ----------------------------------------------------------------------------------------------------------------------
# Extra

def openFromFile(filepath, markets = None):
    """
    <function openFromFile>
    Get (username, password, host, port, dbname) from given file. This file should have syntax with
        <username>
        <password>
        <host> (if host is blank then set to 'localhost')
        <port> (if port is blank then set to 5432)
        <dbname> (if dbname is blank then set to default value (PriceBase.defaultDBname))
    :return: PriceBase object
    """
    with open(filepath) as file:
        username, password, host, port, dbname = [c.strip(' ') for c in file.read().split("\n")]
        if not host: host = "localhost"
        if not port or not port.isdigit(): port = PriceBase.defaultPortNumber
        else: port = int(port)
        if not dbname: dbname = PriceBase.defaultDBname
    return PriceBase(username, password, host = host, port = port, dbname = dbname, additionalMarkets = markets)

# ----------------------------------------------------------------------------------------------------------------------
# Testing

if __name__ == "__main__":
    PDB = openFromFile("awsdb.authkey")

    # Get data test
    '''
    data = PDB.getBetweenTimestamps("Bitflyer", "USD", "BTC", 1, datetime.min, datetime.max)
    print("%d rows found" % len(data))
    for timestamp in data:
        o, h, l, c, v = data[timestamp]
       print("%s -> %s / %s / %s / %s / %s" % (timestamp, o, h, l, c, v))
    '''

    # Grant test
    PDB.grant("ALL", "mcdic", True)

    # CSV copyfile format test
    '''
    data = PDB.getBetweenTimestamps("Bitflyer", "USD", "BTC", 1, datetime.min, datetime.max)
    with open("copy_test.csv", "w") as csvtestfile:
        maxlimit = 10
        for i in range(maxlimit):
            t, (o, h, l, c, v) = data.popitem()
            csvtestfile.write("%s,%s,%s,%s,%s,%s" % (t, o, h, l, c, v))
            if i < maxlimit-1:
                csvtestfile.write("\n")
    print(PDB.fastCopyOHLCV("Bitflyer", "USD", "BTC", 1, "copy_test.csv"))
    '''


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