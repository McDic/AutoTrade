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
import atexit

# External libraries
import pytz
import psycopg2, psycopg2.sql # Passing table name as variable

# Custom libraries

# ----------------------------------------------------------------------------------------------------------------------
# Pricebase

def PTNQ(query: str, tableName: str) -> psycopg2.sql.Composed:
    """
    <function PTNQ>
    Generate new query from given query and table name variables.
    :return: New query
    """
    return psycopg2.sql.SQL(query).format(psycopg2.sql.Identifier(tableName))

class PriceBase:
    """
    <class PriceBase>
    Base database object to store price data.
    """

    defaultDBname = "PriceBase"
    defaultPortNumber = 5432 # Default port number for PostgreSQL
    baseMinuteIntervals = {1,} # Base minute intervals to handle

    # ------------------------------------------------------------------------------------------------------------------
    # Constructors

    def __init__(self, username: str, password: str, dbname: str = defaultDBname,
                 host: str = "localhost", port: int = defaultPortNumber,
                 additionalMarkets: dict = None):

        # Create connections
        self.connection = psycopg2.connect(user = username, password = password, dbname = dbname,
                                           host = host, port = port)
        del username, password
        atexit.register(terminateDBSessionAtExit, session = self)

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
                if tablename.startswith("PriceData_"): # Syntax should be PRICEDATA_(EXCHANGE)_(BASE)_(QUOTE)_(aggregate)mins
                    _, exchange, base, quote, minuteInterval = [c.strip(" ") for c in tablename.split("_")]
                    minuteInterval = int(minuteInterval.replace("mins", ""))
                    if exchange not in self.markets: self.markets[exchange] = {}
                    if base not in self.markets[exchange]: self.markets[exchange][base] = {}
                    if quote not in self.markets[exchange][base]: self.markets[exchange][base][quote] = deepcopy(PriceBase.baseMinuteIntervals)
                    self.markets[exchange][base][quote].add(minuteInterval)

        # Create tables
        self.addMarketTables()

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
        return "PriceData_%s_%s_%s_%dmins" % (exchange, base, quote, minuteInterval)

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
                            cursor.execute(PTNQ("""
                                CREATE TABLE IF NOT EXISTS {} (
                                timestamp TIMESTAMPTZ PRIMARY KEY,
                                open NUMERIC(24, 8) NOT NULL,
                                high NUMERIC(24, 8) NOT NULL,
                                low NUMERIC(24, 8) NOT NULL,
                                close NUMERIC(24, 8) NOT NULL,
                                volume NUMERIC(24, 8) NOT NULL,
                                CHECK(volume > 0), 
                                CHECK(timestamp <= NOW())
                            );""", PriceBase.tableName(exchange, base, quote, minuteInterval)))
        self.connection.commit()

    # ------------------------------------------------------------------------------------------------------------------
    # Grant privileges

    def grant(self, privilege: str, username: str = "PUBLIC", give: bool = True):
        assert privilege.upper() in ("SELECT", "INSERT", "UPDATE", "DELETE", "RULE", "ALL")
        try:
            with self.connection.cursor() as cursor:
                for exchange in self.markets:
                    for base in self.markets[exchange]:
                        for quote in self.markets[exchange][base]:
                            for minuteInterval in self.markets[exchange][base][quote]:
                                IDtuple = (psycopg2.sql.Identifier(self.tableName(exchange, base, quote, minuteInterval)),
                                           psycopg2.sql.Identifier(username))
                                if give: cursor.execute(psycopg2.sql.SQL("GRANT %s ON {0} TO {1}" % (privilege.upper(),)).format(*IDtuple))
                                else: cursor.execute(psycopg2.sql.SQL("REVOKE %s ON {0} FROM {1}" % (privilege.upper(),)).format(*IDtuple))
            self.connection.commit()
        except psycopg2.Error as err:
            print("Error (%s) occured while doing <%s>" % (err, str(cursor.query)))
            self.connection.rollback()

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
                cursor.execute(PTNQ('SELECT "open", "high", "low", "close", "volume" FROM {} WHERE "timestamp" = %s;',
                                    self.tableName(exchange, base, quote, minuteInterval)), (timestamp,))
                result = cursor.fetchall()
                self.connection.commit()
                assert len(result) < 2
                return result[0][1:6] if result else None
        except psycopg2.Error as err:
            self.connection.rollback()
            print("Error occured in getSingle: <%s> // query: <%s>" % (err, cursor.query))
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
                cursor.execute(PTNQ('SELECT * FROM {} WHERE timestamp BETWEEN %s AND %s ORDER BY timestamp ASC;',
                                    PriceBase.tableName(exchange, base, quote, minuteInterval)),
                               (startTime, endTime))
                self.connection.commit()
                result = {}
                for timestamp, O, H, L, C, V in cursor: result[timestamp] = (O, H, L, C, V)
                return result
        except psycopg2.Error as err:
            self.connection.rollback()
            print("Error occured in getBetweenTimestamps: <%s> // query: <%s>" % (err, cursor.query))
            return {}

    # ------------------------------------------------------------------------------------------------------------------
    # Insert or Update OHLCV data

    def addSingleOHLCV(self, exchange: str, base: str, quote: str, minuteInterval: int,
                       timestamp: datetime, O: Decimal, H: Decimal, L: Decimal, C: Decimal, V: Decimal,
                       override: bool = True, showDetailedProgress: bool = False) -> bool:
        """
        <method PriceBase.addSingleOHLCV>
        Insert or update single (timestamp, O, H, L, C, V) into database.
        :return: If the operation was successful
        """

        assert minuteInterval in self.availableIntervals(exchange, base, quote) # Data table should be available
        assert round(timestamp.timestamp()) % (minuteInterval * 60) == 0 # Timestamp must not violate minuteInterval offset
        tableName = self.tableName(exchange, base, quote, minuteInterval)
        try: # Try with insertion
            with self.connection.cursor() as cursor:
                if override:
                    cursor.execute(PTNQ("""
                        INSERT INTO {} ("timestamp", "open", "high", "low", "close", "volume")
                        VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT ("timestamp")
                        DO UPDATE SET ("open", "high", "low", "close", "volume") = 
                        (EXCLUDED."open", EXCLUDED."high", EXCLUDED."low", EXCLUDED."close", EXCLUDED."volume");
                    """, tableName), (timestamp, O, H, L, C, V))
                else:
                    cursor.execute(PTNQ("""
                        INSERT INTO {} ("timestamp", "open", "high", "low", "close", "volume")
                        VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;
                    """, tableName), (timestamp, O, H, L, C, V))
                self.connection.commit()
            return True
        except psycopg2.Error as err:
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
        Fast bulk insertion using COPY query with given file. The given file should matches like:
        <timestamp> <sep> <open> <sep> <high> <sep> <low> <sep> <close> <sep> <volume> [\n ... (repeat)]
        :return: If the query was successful.
        """
        assert minuteInterval in self.availableIntervals(exchange, base, quote) # Data table should be available
        tableName = self.tableName(exchange, base, quote, minuteInterval)
        with self.connection.cursor() as cursor, open(filename) as datafile:
            try:
                if showDetailedProgress:
                    print("Fast copying %s into (%s, %s, %s, %d)" % (filename, exchange, base, quote, minuteInterval))
                # 1. Create temporary table
                cursor.execute(psycopg2.sql.SQL("CREATE TABLE IF NOT EXISTS {0} (LIKE {1});").format(
                    psycopg2.sql.Identifier(tempTableName), psycopg2.sql.Identifier(tableName)))
                if showDetailedProgress: print("Temp table created(or already exist)")
                # 2. Delete all rows from temp table using truncate
                cursor.execute(PTNQ("TRUNCATE {}", tempTableName))
                if showDetailedProgress: print("Truncated temp table")
                # 3. Copy
                cursor.copy_expert(psycopg2.sql.SQL("COPY {0} FROM STDIN (DELIMITER {1})").format(
                    psycopg2.sql.Identifier(tempTableName), psycopg2.sql.Literal(separate)), datafile)
                if showDetailedProgress: print("Copied original data to temp table")
                # 4. Move all data from PriceDataTemp to target table
                if override:
                    cursor.execute(psycopg2.sql.SQL("""
                        INSERT INTO {0} (SELECT * FROM {1}) ON CONFLICT ("timestamp")
                        DO UPDATE SET   ("open", "high", "low", "close", "volume") = 
                        (EXCLUDED."open", EXCLUDED."high", EXCLUDED."low", EXCLUDED."close", EXCLUDED."volume");
                    """).format(psycopg2.sql.Identifier(tableName), psycopg2.sql.Identifier(tempTableName)))
                    if showDetailedProgress: print("Moved data to actual table with conflict override action")
                else:
                    cursor.execute(psycopg2.sql.SQL("""
                        INSERT INTO {0} (SELECT * FROM {1}) ON CONFLICT ("timestamp") DO NOTHING;
                    """).format(psycopg2.sql.Identifier(tableName), psycopg2.sql.Identifier(tempTableName)))
                    if showDetailedProgress: print("Moved data to actual table without conflict override action")
                # 5. Truncate again
                cursor.execute(PTNQ("TRUNCATE {};", tempTableName))
                if showDetailedProgress: print("Retruncated temp table")
            except psycopg2.Error as err:
                self.connection.rollback()
                if showDetailedProgress: print("psycopg2.Error <%s> occured while doing query <%s>" % (err, str(cursor.query)))
                return False
            else: self.connection.commit()
        return True

# ----------------------------------------------------------------------------------------------------------------------
# Extra

def terminateDBSessionAtExit(session: PriceBase):
    """
    <function terminateDBSessionAtExit>
    Terminate the session at the exit of program.
    """
    try: session.connection.close()
    except psycopg2.Error: pass

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