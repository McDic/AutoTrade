"""
<module AutoTrade.database.base>
Abstract base of all database connection.
"""

# ----------------------------------------------------------------------------------------------------------------------
# Libraries

# Standard libraries
import logging
import asyncio

# External libraries
import asyncpg, asyncpg.exceptions

# Custom libraries
from connection.base import AbstractConnection
import connection.errors as cerr

# ----------------------------------------------------------------------------------------------------------------------
# Base database

class AbstractPGDBConnectionClass(AbstractConnection):
    """
    <class AbstractPGDBConnectionClass> derived from AbstractConnection
    Abstract base of all Postgres database connections.
    """

    defaultHost = "localhost"
    defaultPortNumber = 5432  # Default port number for PostgreSQL

    # ------------------------------------------------------------------------------------------------------------------
    # Constructor

    def __init__(self, userName: str, password: str, DBname: str,
                 host: str = defaultHost, port: int = defaultPortNumber,
                 connectionName: str = None, callLimits: dict = None, keys: dict = None):

        # Parent class initialization
        if connectionName is None: connectionName = "no_named"
        super().__init__(connectionName = connectionName, callLimits = callLimits, keys = keys)
        self.host, self.username, self.port, self.DBname = host, userName, port, DBname

        # Async initialization is not completed, you need to it.
        self.__initialized_async = False

    async def _init_async(self, host: str, port: int, userName: str, password: str, DBname: str):
        """
        <method AbstractPGDBConnectionClass._init_async>
        Initialize asynchronous things.
        """

        # Don't do same thing twice
        if self.__initialized_async: return
        self.__initialized_async = True

        # Init async PostgreSQL connection
        self.connection = await asyncpg.connect(host = host, port = port, user = userName, password = password, database = DBname)

        # Exact server version
        exactServerVersion = self.connection.get_server_version()
        self.serverVersion = f"{exactServerVersion.major}.{exactServerVersion.minor}.{exactServerVersion.micro}"
        self.PID = self.connection.get_server_pid()

    # ------------------------------------------------------------------------------------------------------------------
    # Representation

    def __str__(self):
        return "Abstract PostgreSQL %s Connection [%s]: Connected to %s@%s:%d/%s (PID = %d)" % \
               (self.serverVersion, self.name, self.username, self.host, self.port, self.DBname, self.PID)
    __repr__ = __str__

    # ------------------------------------------------------------------------------------------------------------------
    # Query: Helpers and light queries

    @staticmethod
    def RTN(command: str, *tableNames, tableSign: str = '{T}', varTableSign: str = '{vT}'):
        """
        <method AbstractPGDBConnectionClass.RTN>
        RTN = Replace part by Table Name.
        Generate query by replacing some part of command to table names.
        Use this method when you need to specify table name in query by variable,
            don't use this method when you need to pass table name itself as query variable.
        :param command: Given command to insert table names.
        :param tableNames: Table names.
        :param tableSign: Considered as table names.
        :return: Generated query.
        """
        if '%' in tableSign: raise cerr.InvalidValueError("'%' is in tableSign")
        return command.replace("%", "%%").replace(tableSign, "\"%s\"").replace(varTableSign, "'\"%s\"'") % tableNames

    async def getColumns(self, tableName: str):
        """
        <async method AbstractPGDBConnectionClass.getColumns>
        Return all column names for given table name.
        If given table not exists, error will be raised automatically.
        """
        async with self.connection.transaction():
            columns = [row[0] for row in await self.connection.fetch(
                "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = $1", tableName)]
        return columns

    async def getPrimaryKeys(self, tableName: str):
        """
        <async method AbstractPGDBConnectionClass.getPrimaryKeys>
        Return all primary key names for given table name.
        If given table not exists, error will be raised automatically.
        """
        async with self.connection.transaction():
            primaryKeys = [row[0] for row in await self.connection.fetch(self.RTN("""
                SELECT a.attname AS key_name
                FROM   pg_index i
                JOIN   pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                WHERE  i.indrelid = {vT}::regclass AND i.indisprimary;
            """, tableName))]
        print("Fetched primary keys: %s" % (primaryKeys,))
        return primaryKeys

    # ------------------------------------------------------------------------------------------------------------------
    # Query: Generic and heavy queries

    async def pushFile(self, fileName, tableName, columns: tuple = None, timeout: float = None,
                       delimiter = ",", override: bool = True):
        """
        <async method AbstractPGDBConnectionClass.pushFile>
        Copy data from file to given table.
        Precise of file data syntax: (Note that ',' is base delimiter, other delimiter can be used.)
            a11, a12, a13, ..., a1c
            a21, a22, a23, ..., a2c
            ...  ...  ...       ...
            ar1, ar2, ar3, ..., arc
        :param fileName: Name of file to copy. The file will be opened in 'rb' mode.
            Each row of content of file should be ordered by given columns order or default order.
        :param tableName: The name of target table.
        :param columns: Provide order of columns of each row of content of given file.
        :param timeout: Query timeout deadline. None for non-deadline.
        :param override: Decide to override conflicted data or not.
        """

        # Step 1: Find all columns and primary keys and validate given column order(if provided) is valid.
        tableColumns = await self.getColumns(tableName)
        primaryKeys = await self.getPrimaryKeys(tableName)
        if columns: # If column order given, all column name should be distinct and all primary key should be happened.
            pass

        # Step 2: Copy data to empty temporary table,
        async with self.connection.transaction():

            # Step 2-1: Create temporary table with very rare name, it's dropped after commit.
            tempTableName = "_temp_table_name_that_used_in_AbstractPDGBConnection_"
            await self.connection.execute(
                # self.RTN("DROP TABLE IF EXISTS {T}", tempTableName),
                self.RTN("CREATE TEMPORARY TABLE {T} (LIKE {T}) ON COMMIT DROP", tempTableName, tableName),
                timeout = timeout
            )

            # Step 2-2: Copy file data to temp table.
            await self.connection.copy_to_table(tempTableName, columns = columns,
                source = open(fileName, "rb"), delimiter = delimiter, timeout = timeout)

            # Step 2-3: Copy rows from temp table to main table, and drop temp table again.
            if override:
                if columns: updateColumns = tuple(columns)
                else: updateColumns = [column for column in tableColumns if column not in primaryKeys]
                if len(updateColumns) > 1:
                    updateQuerySuffix = "(" + ", ".join("\"" + primaryKey + "\"" for primaryKey in primaryKeys) \
                        + ") DO UPDATE SET (" + ", ".join("\"" + column + "\"" for column in updateColumns) \
                        + ") = (" + ", ".join("EXCLUDED.\"" + column + "\"" for column in updateColumns) + ")"
                else:
                    updateQuerySuffix = "(" + ", ".join("\"" + primaryKey + "\"" for primaryKey in primaryKeys) \
                        + ") DO UPDATE SET \"" + updateColumns[0] + "\" = EXCLUDED.\"" + updateColumns[0] + "\""
            else: updateQuerySuffix = "DO NOTHING"
            lastQuery = self.RTN("INSERT INTO {T} (SELECT * FROM {T}) ON CONFLICT " + updateQuerySuffix, tableName, tempTableName)
            await self.connection.execute(lastQuery, timeout = timeout)

    # ------------------------------------------------------------------------------------------------------------------

async def AbstractPGDBConnection(userName: str, password: str, DBname: str,
                                 host: str = AbstractPGDBConnectionClass.defaultHost,
                                 port: int = AbstractPGDBConnectionClass.defaultPortNumber, **kwargs):
    """
    <function AbstractPGDBConnection>
    Construct and return Abstract PostgreSQL Connection.
    I recommend you to use this function to get Connection object instead of direct instantiation.
    """
    connection = AbstractPGDBConnectionClass(userName = userName, password = password, DBname = DBname,
                                             host = host, port = port, **kwargs)
    await connection._init_async(host, port, userName, password, DBname)
    return connection

# ----------------------------------------------------------------------------------------------------------------------
# Functionality testing

if __name__ == "__main__":

    from pprint import pprint
    print(AbstractPGDBConnectionClass.__doc__)

    async def run():
        PGDB = await AbstractPGDBConnection("McDicBot", "1234", "postgres", connectionName ="TestConnection")
        result1 = await PGDB.connection.execute(PGDB.RTN("TRUNCATE {T}", "TEST3"))
        await PGDB.connection.execute("INSERT INTO \"TEST3\" VALUES ($1, $2) ON CONFLICT DO NOTHING", 103578, -7.2)
        result3 = await PGDB.connection.fetch(PGDB.RTN("SELECT * FROM {T}", "TEST3"))
        pprint(result1)
        pprint(result3)
        await PGDB.pushFile("../../test/data/test_insert.txt", "TEST3", override = True)
        result_again = await PGDB.connection.fetch(PGDB.RTN("SELECT * FROM {T}", "TEST3"))
        pprint(result_again)

    asyncio.get_event_loop().run_until_complete(run())