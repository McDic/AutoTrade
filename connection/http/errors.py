"""
    <module AutoTrade.connection.http.errors>
    This module is used to list base errors of AbstractHTTPConnection submodule.
"""

# ----------------------------------------------------------------------------------------------------------------------
# Libraries

# ----------------------------------------------------------------------------------------------------------------------
# Error classes for AbstractHTTPConnection


class HTTPConnectionError(Exception):
    """
    <class AutoTradeConnectionError> inherited from Exception
    Abstract base of all connection's errors.
    """

class ServiceError(HTTPConnectionError):
    """
    <class ServiceError> inherited from HTTPConnectionError
    Abstract base of all service errors from server response
    """

class DDoSProtection(HTTPConnectionError):
    """
    <class DDoSProtection> inherited from HTTPConnectionError
    When server refuses to response related to DDos protection
    """

class ServiceNotAvailable(HTTPConnectionError):
    """
    <class ServiceNotAvailable> inherited from HTTPConnectionError
    When server is not available to response
    """

class RequestTimeout(HTTPConnectionError):
    """
    <class RequestTimeout> inherited from RequestTimeout
    When server has no response in request timeout
    """

class AuthenticationError(HTTPConnectionError):
    """
    <class AuthenticationError> inherited from HTTPConnectionError
    Abstract base of all authentication errors.
    """

# ----------------------------------------------------------------------------------------------------------------------
# Testing

if __name__ == "__main__":
    pass