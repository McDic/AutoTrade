"""
<module AutoTrade.database.base>
Abstract base of all database connection.
"""

# ----------------------------------------------------------------------------------------------------------------------
# Libraries

# Standard libraries
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
                 connectionName: str = None, callLimits: dict = None):

        # Parent class initialization
        if connectionName is None: connectionName = "no_named"
        super().__init__(connectionName = connectionName, callLimits = callLimits)
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
        self.connection = await asyncpg.connect(host = host, port = port, user = userName,
                                                password = password, database = DBname)

        # Exact server version
        exactServerVersion = self.connection.get_server_version()
        self.serverVersion = f"{exactServerVersion.major}.{exactServerVersion.minor}.{exactServerVersion.micro}"
        self.PID = self.connection.get_server_pid()

    # ------------------------------------------------------------------------------------------------------------------
    # Representation

    def __str__(self) -> str:
        return "Abstract PostgreSQL %s Connection [%s]: Connected to %s@%s:%d/%s (PID = %d)" % \
               (self.serverVersion, self.name, self.username, self.host, self.port, self.DBname, self.PID)
    __repr__ = __str__

    # ------------------------------------------------------------------------------------------------------------------
    # Query: Helpers and light queries

    @staticmethod
    def _isCorrectSign(sign: str) -> bool:
        """
        <method AbstractPGDBConnectionClass._isCorrectSign>
        :return: If given sign is correct or not.
        """
        if sign.startswith('{') and sign.endswith('}'):
            if "\n" in sign or "$" in sign: return False
            else: return True
        return False

    defaultNameSign, defaultVarNameSign = "{T}", "{vT}"
    @staticmethod
    def RN(query: str, *names, nameSign: str = defaultNameSign, varNameSign: str = defaultVarNameSign) -> str:
        """
        <static method AbstractPGDBConnectionClass.RN>
        RN = Replace part by names
        Generate query by replacing some part of command to table names.
        Use this method when you need to specify table name in query by variable,
            don't use this method when you need to pass table name itself as query variable.
        :param query: Given command to insert table names.
        :param names: Table names.
        :param nameSign: Considered query's part as names.
        :return: Generated query.
        """
        if not AbstractPGDBConnectionClass._isCorrectSign(nameSign):
            raise cerr.InvalidValueError("Given nameSign is not correct(%s)" % (nameSign,))
        elif not AbstractPGDBConnectionClass._isCorrectSign(varNameSign):
            raise cerr.InvalidValueError("'%' is in varNameSign")
        return query.replace("%", "%%").replace(nameSign, "\"%s\"").replace(varNameSign, "'\"%s\"'") % names

    async def getColumns(self, tableName: str) -> list:
        """
        <async method AbstractPGDBConnectionClass.getColumns>
        Return all column names for given table name.
        If given table not exists, error will be raised automatically.
        """
        async with self.connection.transaction():
            columnNames = await self.connection.fetch(
                "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = $1", tableName)
        return [row[0] for row in columnNames]

    async def getPrimaryKeys(self, tableName: str) -> list:
        """
        <async method AbstractPGDBConnectionClass.getPrimaryKeys>
        Return all primary key names for given table name.
        If given table not exists, error will be raised automatically.
        """
        async with self.connection.transaction():
            primaryKeys = await self.connection.fetch(self.RN("""
                SELECT a.attname AS key_name
                FROM   pg_index i
                JOIN   pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                WHERE  i.indrelid = {vT}::regclass AND i.indisprimary;
            """, tableName))
        return [row[0] for row in primaryKeys]

    # ------------------------------------------------------------------------------------------------------------------
    # Query: Generic and heavy queries

    async def execute(self, query: str, tableNames: tuple = (), *args,
                      toleratedExceptions: tuple = (), timeout: float = None, fetch: bool = False,
                      tableSign: str = defaultNameSign, varTableSign: str = defaultVarNameSign):
        """
        <async method AbstractPGDBConnectionClass.execute>
        :param query: Base query to execute.
        :param tableNames, tableSign, varTableSign: Table names and table signs to replace.
        :param args: Argument values passed into 'self.connection.execute'.
        :param toleratedExceptions: List of tolerated exceptions.
            If one of the exception in toleratedExceptions raised, it's tolerated.
        :param timeout: Timeout used in 'self.connection.execute'.
        :return: If fetch is True, return if the given query was executed or not. Otherwise, return fetched result.
        """
        query = self.RN(query, *tableNames, nameSign= tableSign, varNameSign= varTableSign)
        # print("Will try query: %s / args: %s" % (query, args))
        try:
            async with self.connection.transaction():  # Transaction or savepoint begin
                if not fetch: await self.connection.execute(query, *args, timeout = timeout)
                else: return await self.connection.fetch(query, *args, timeout = timeout)
        except toleratedExceptions: return False
        else:
            if not fetch: return True
            else: return None

    async def pushFile(self, fileName, tableName, columns: tuple = None, timeout: float = None,
                       delimiter = ",", override: bool = True):
        """
        <async method AbstractPGDBConnectionClass.pushFile>
        Copy data from file to given table with resolving conflicts.
        Precise of file data syntax: (Note that ',' is base delimiter, other delimiter can be used.)
            a11, a12, a13, ..., a1c
            a21, a22, a23, ..., a2c
            ...  ...  ...       ...
            ar1, ar2, ar3, ..., arc
        :param fileName: Name of file to copy. The file will be opened in 'rb' mode.
            Each row of content of file should be ordered by given columns order or default order.
        :param tableName: The name of target table.
        :param columns: Provide order of columns of each row of content of given file.
            Precise syntax: None | (c[1], c[2]..., c[c]) where c[i] is name of i-th column in data file.
        :param timeout: Query timeout deadline. None for non-deadline.
        :param override: Decide to override conflicted data or not.
        """

        # Copy data to empty temporary table,
        async with self.connection.transaction():

            # Step 1: Create temporary table with very rare name, it's dropped after commit.
            tempTableName = "_temp_table_name_that_used_in_AbstractPGDBConnection_"
            await self.execute("CREATE TEMPORARY TABLE {T} (LIKE {T}) ON COMMIT DROP",
                               (tempTableName, tableName), timeout = timeout)

            # Step 2: Copy file data to temp table.
            with open(fileName, "rb") as sourceFile:
                await self.connection.copy_to_table(tempTableName, columns = columns,
                    source = sourceFile, delimiter = delimiter, timeout = timeout)

            # Step 3: Generate insert query suffix by given columns and calculating primary keys and total columns.
            if override:
                primaryKeys = set(await self.getPrimaryKeys(tableName)) # Get primary keys
                if columns: updateColumns = tuple(columns)
                else:
                    tableColumns = await self.getColumns(tableName) # Get all column names
                    updateColumns = [column for column in tableColumns if column not in primaryKeys]
                if len(updateColumns) > 1:
                    updateQuerySuffix = "(" + ", ".join("\"" + primaryKey + "\"" for primaryKey in primaryKeys) \
                        + ") DO UPDATE SET (" + ", ".join("\"" + column + "\"" for column in updateColumns) \
                        + ") = (" + ", ".join("EXCLUDED.\"" + column + "\"" for column in updateColumns) + ")"
                else:
                    updateQuerySuffix = "(" + ", ".join("\"" + primaryKey + "\"" for primaryKey in primaryKeys) \
                        + ") DO UPDATE SET \"" + updateColumns[0] + "\" = EXCLUDED.\"" + updateColumns[0] + "\""
            else: updateQuerySuffix = "DO NOTHING"

            # Step 4: Copy rows from temp table to main table, and drop temp table again.
            lastQuery = "INSERT INTO {T} (SELECT * FROM {T}) ON CONFLICT " + updateQuerySuffix
            await self.execute(lastQuery, (tableName, tempTableName), timeout = timeout)

    tableGrantPrivileges = ["SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER", "ALL"]
    async def grantTable(self, privilege: str, userName: str = None, tableName: str = None, schema: str = "public",
                         withGrantOption: bool = False, grant: bool = True):
        """
        <async method AbstractPGDBConnectionClass.grantTable>
        Execute grantTable query with given options.
        :param privilege: Privilege to grant or revoke. None or empty string implies "ALL".
        :param userName: User name to grant or revoke. If None given then give to "PUBLIC".
        :param tableName: Table name to specify. If None given then grantTable/revoke on all tables in given schema.
        :param schema: Schema name to specify. Default to "public".
        :param withGrantOption: Determine to give grant option or not.
        :param grant: Determine to grant(True) or revoke(False).
        """
        if not privilege: privilege = "ALL"
        if privilege not in AbstractPGDBConnectionClass.tableGrantPrivileges:
            raise InvalidQueryError("Given privilege(%s) is not valid" % (privilege,))
        query = ("GRANT" if grant else "REVOKE") + " " + privilege + " ON " + \
                (self.RN("TABLE {T}", tableName) if tableName else self.RN("ALL TABLES IN SCHEMA {T}", schema)) + \
                " " + ("TO" if grant else "FROM") + " " + (self.RN("{T}", userName) if userName else "PUBLIC") + \
                (" WITH GRANT OPTION" if withGrantOption else "")
        await self.execute(query)

    async def grantDatabase(self, ): raise NotImplementedError

# ----------------------------------------------------------------------------------------------------------------------
# Exceptions

class AbstractPGDBError(cerr.AutoTradeConnectionError, asyncpg.exceptions.PostgresError):
    """
    <class AbstractPGDBError> inherited from AutoTradeConnectionError, asyncpg.exceptions.PostgresError
    Abstract base of all exceptions for AbstractPGDBConnectionClass.
    """

class InvalidQueryError(AbstractPGDBError, asyncpg.exceptions.QueryCanceledError):
    """
    <class InvalidQueryError> inherited from AbstractPGDBError, asyncpg.exceptions.QueryCanceledError
    Raised when the query is invalid.
    """

# ----------------------------------------------------------------------------------------------------------------------
# Asynchronized init binder
async def AbstractPGDBConnection(userName: str = None, password: str = None, DBname: str = None,
                                 host: str = AbstractPGDBConnectionClass.defaultHost,
                                 port: int = AbstractPGDBConnectionClass.defaultPortNumber,
                                 fileName: str = None, **kwargs) -> AbstractPGDBConnectionClass:
    """
    <function AbstractPGDBConnection>
    Construct and return Abstract PostgreSQL Connection asynchronously.
    I recommend you to use this function to get Connection object instead of direct instantiation.
    """
    if userName is None and password is None and DBname is None: # Should recover information from file
        if fileName is None: raise cerr.InvalidError("None of mandatory arguments were given.")
        with open(fileName, "r") as authFile:
            userName, password, host, port, DBname = [c.strip(' ') for c in authFile.read().split("\n")]
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

        PGDB = await AbstractPGDBConnection(fileName = "awsdb.authkey")

    async def run_m43ng():

        PGDB = await AbstractPGDBConnection(fileName = "awsdb_m43ng.authkey")
        pprint(await PGDB.connection.fetch("SELECT * FROM \"PriceData_Bitfinex_USD_BTC_1mins\" LIMIT 100"))

    asyncio.get_event_loop().run_until_complete(run())