"""
    <module AutoTrade.connection.errors>
    This module is used to list base errors of connection submodule.
"""

# ----------------------------------------------------------------------------------------------------------------------
# Libraries

# ----------------------------------------------------------------------------------------------------------------------
# Error classes for AbstractConnection

class AutoTradeConnectionError(Exception):
    """
    <class AutoTradeConnectionError> inherited from Exception
    Abstract base of all connection's error.
    """

class CallLimitExceededError(AutoTradeConnectionError):
    """
    <class CallLimitExceededError> inherited from AutoTradeConnectionError
    Used when connection's call limit exceeded.
    """
    def __init__(self, connectionName: str, fieldName: str):
        super().__init__("Connection [%s] call [%s] weight limit exceeded" % (connectionName, fieldName))

class InvalidError(AutoTradeConnectionError):
    """
    <class InvalidError> inherited from AutoTradeConnectionError
    Used when invalid argument or value is passed to connection's method.
    But simple assertions can be raised by AssertionError, not this error.
    """

class MarketNotSupported(AutoTradeConnectionError):
    """
    <class MarketNotSupported> inherited from AutoTradeConnectionError
    Used when given market is not supported.
    """
    def __init__(self, exchange: str, base: str, quote: str):
        super().__init__("Market (%s, %s <-> %s) not supported" % (exchange, base, quote))

# ----------------------------------------------------------------------------------------------------------------------
# __all__
