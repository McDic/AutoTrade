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

class CallRelatedError(AutoTradeConnectionError):
    """
    <class CallRelatedError> inherited from AutoTradeConnectionError
    Abstract base of all errors about internal rate limiting.
    """

class CallLimitExceededError(CallRelatedError):
    """
    <class CallLimitExceededError> inherited from CallRelatedError
    Used when connection's call limit exceeded.
    """
    def __init__(self, connectionName: str, fieldName: str):
        super().__init__("Connection [%s] call [%s] weight limit exceeded" % (connectionName, fieldName))

class CallCancelled(CallRelatedError):
    """
    <class CallCancelled> inherited from CallRelatedError
    Used when the call is cancelled by internal issue.
    """

class InvalidError(AutoTradeConnectionError):
    """
    <class InvalidError> inherited from AutoTradeConnectionError
    Used when invalid argument or value is passed to connection's method.
    """

class InvalidTypeError(InvalidError, TypeError):
    """
    <class InvalidTypeError> inherited from InvalidError
    Used when given value's type is invalid.
    """

class InvalidValueError(InvalidError, ValueError):
    """
    <class InvalidValueError> inherited from ValueError
    Used when given value is invalid.
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

# ----------------------------------------------------------------------------------------------------------------------
# Testing

if __name__ == "__main__":
    pass