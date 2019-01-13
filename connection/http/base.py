"""
<module AutoTrade.connection.http.base>
This module is used to describe the abstraction of all type of HTTP connection.
"""
# ----------------------------------------------------------------------------------------------------------------------
# Libraries

# Standard libraries
import time

# External libraries
from requests_futures.sessions import FuturesSession

# Custom libraries
import connection.base, connection.errors

# ----------------------------------------------------------------------------------------------------------------------
# Abstract base

class AbstractHTTPConnection(connection.base.AbstractConnection):
    """
    <class AbstractHTTPConnection>
    Abstract base of all HTTP connections.
    """

    def __init__(self, connectionName: str, baseURL: str,
                 callLimits = None, key = None, maxConcurrent = 8):
        """
        <method AbstractHTTPConnection.__init__>
        :param connectionName:  The name of this connection.
        :param baseURL:         The base endpoint of target of this connection.
        :param callLimits:      Same argument as AbstractConnection.
        :param key:             Same argument as AbstractConnection.
        :param maxConcurrent:   Maximum concurrent workers to send asynchronous HTTP request.
        """

        # Parent class initialization
        super().__init__(connectionName, callLimits = callLimits, key = key)

        # Base URL set
        self.baseURL = baseURL

        # Asynchronous session
        self.session = FuturesSession(max_workers = maxConcurrent)
        self.dataProcessID = 0 # Nonce value used to data process

    def __str__(self):
        return "Abstract HTTP Connection [%s]" % (self.name,)

    def targetURL(self, endpoint: str):
        """
        <method AbstractHTTPConnection.targetURL>
        Generate URL with given endpoint.
        :param endpoint:
        :return: URL targeting to given endpoint
        """

        return self.baseURL + "/" + endpoint

    def request(self, mode: str, endpoint: str,
                callFieldName: str, callWeight: int,
                params = None, data = None, header = None):
        """
        <method AbstractHTTPConnection.request>
        Create asynchronous request.
        :param mode:            Restful mode. One of the POST, GET, PUT, DELETE.
        :param endpoint:        Target endpoint.
        :param callFieldName:   Field name for call limit.
        :param callWeight:      Weight for call limit.
        :param params:          Parameters for HTTP request.
        :param data:            Data for HTTP request.
        :param header:          Header for HTTP request.
        :return Asynchronous request object. To open result, eval "<return value>.result()"
        """

        # Call field checking
        if callFieldName and not self.isPossibleCall(callFieldName, callWeight):
            raise connection.errors.CallLimitExceededError(self.name, callFieldName)

        # Make asynchronous request
        targetURL = self.targetURL(endpoint)
        if mode == "POST":      return self.session.post(targetURL, data = data, header = header)
        elif mode == "GET":     return self.session.get(targetURL, params = params)
        elif mode == "PUT":     return self.session.put(targetURL, data = data)
        elif mode == "DELETE":  return self.session.delete(targetURL)
        else:                   raise connection.errors.InvalidArgumentError("Invalid mode(%s) to request" % (mode,))

    def responseHandle(self, req, mode: str):
        """
        <method AbstractHTTPConnection.responseHandle>
        :param req: Request to handle.
        :param mode: Handling mode, usually equivalent to endpoint.
        :return: Processed data from response.
        """
        raise NotImplementedError("%s is not ready to handle any HTTP responses" % (self.name,))