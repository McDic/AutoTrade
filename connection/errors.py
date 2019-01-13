"""
    <module AutoTrade.connection.errors>
    This module is used to list base errors of connection submodule.
"""

# ----------------------------------------------------------------------------------------------------------------------
# Libraries

# ----------------------------------------------------------------------------------------------------------------------
# Error classes for AbstractConnection

class AbstractConnectionError(ConnectionError):
    """
    <class AbstractConnectionError> inherited from ConnectionError
    Abstract base of all connection's error.
    """

class CallLimitExceededError(AbstractConnectionError):
    """
    <class CallLimitExceededError> inherited from AbstractConnectionError
    Used when connection's call limit exceeded.
    """

    def __init__(self, connectionName: str, fieldName: str):
        super().__init__("Connection [%s] call [%s] weight limit exceeded" % (connectionName, fieldName))

class InvalidArgumentError(AbstractConnectionError):
    """
    <class InvalidArgumentError> inherited from AbstractConnectionError, ValueError
    Used when invalid argument or value is passed to connection's method.
    But simple assertions can be raised by AssertionError, not this error.
    """

# ----------------------------------------------------------------------------------------------------------------------
# __all__
__all__ = ["AbstractConnectionError", "CallLimitExceededError", "InvalidArgumentError"]
