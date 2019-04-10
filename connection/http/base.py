"""
<module AutoTrade.connection.http.base>
This module is used to describe the abstraction of all type of HTTP connection.
"""
# ----------------------------------------------------------------------------------------------------------------------
# Libraries

# Standard libraries
import re

# External libraries
import aiohttp
import asyncio

# Custom libraries
import connection.base, connection.errors

# ----------------------------------------------------------------------------------------------------------------------
# Abstract base

class AbstractHTTPConnection(connection.base.AbstractConnection):
    """
    <class AbstractHTTPConnection>
    Abstract base of all HTTP connections.
    """

    # ----------------------------------------------------------------------------------------------------------------------
    # HTTP exceptions codes
    
    httpExceptions = {
        422 : 'ServiceError',
        418 : 'DDoSProtection',
        429 : 'DDoSProtection',
        404 : 'ServiceNotAvailable',
        409 : 'ServiceNotAvailable',
        500 : 'ServiceNotAvailable',
        501 : 'ServiceNotAvailable',
        502 : 'ServiceNotAvailable',
        520 : 'ServiceNotAvailable',
        521 : 'ServiceNotAvailable',
        522 : 'ServiceNotAvailable',
        525 : 'ServiceNotAvailable',
        526 : 'ServiceNotAvailable',
        400 : 'ServiceNotAvailable',
        403 : 'ServiceNotAvailable',
        405 : 'ServiceNotAvailable',
        503 : 'ServiceNotAvailable',
        530 : 'ServiceNotAvailable',
        408 : 'RequestTimeout',
        504 : 'RequestTimeout',
        401 : 'AuthenticationError',
        511 : 'AuthenticationError',
    }

    # ----------------------------------------------------------------------------------------------------------------------
    # Base methods

    def __init__(self, baseURL : str, connectionName: str = "AbstractHTTPConnection",
                 callLimits = None, keys = None, timeout : int = None, headers = None,
                 cookies = None, loop = None, hook = None):
        """
        <method AbstractHTTPConnection.__init__>
        :param connectionName:  The name of this connection.
        :param baseURL:         The base endpoint of target of this connection.
        :param callLimits:      Same argument as AbstractConnection.
        :param keys:             Same argument as AbstractConnection.
        :param header:  optional parameter to specialize header
        :param cookies: optional parameter to specialize cookie
        """

        # Parent class initialization
        super().__init__(connectionName, callLimits = callLimits, keys= keys)

        # Base URL set
        self.baseURL = baseURL

        # Setting timeout
        if timeout is None:
            timeout = 1 * 60    # 1 minutes
        self.timeout = aiohttp.ClientTimeout(total=timeout)

        # Setting Loop
        if loop is None:
            self.loop = asyncio.get_event_loop()
        else:
            self.loop = loop
        
        # Initialize header if None
        headers = headers or {}
        # Evading crawling prevention
        if headers is None or 'User-Agent' not in headers:
            headers.update({'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Mobile Safari/537.36'})
        # Quicker network response
        headers.update({'Accept-Encoding': 'gzip, deflate'})

        # Initialize session
        self.session = None
        async def initSession(headers = headers, cookies = cookies):
            if self.session is None:
                self.session = aiohttp.ClientSession(headers = headers, cookies = cookies, timeout = self.timeout)
        self.loop.run_until_complete(initSession(headers = headers, cookies = cookies))\



    def __str__(self): return "Abstract HTTP Connection [%s]" % (self.name,)
    
    # ------------------------------------------------------------------------------------------------------------------
    # Termination

    def terminate(self):
        async def closeSession():
            if self.session is not None:
                await self.session.close()
        self.loop.run_until_complete(closeSession())
        self.session = None            

    # ----------------------------------------------------------------------------------------------------------------------
    # Class methods

    def targetURL(self, endpoint: str):
        """
        <method AbstractHTTPConnection.targetURL>
        Generate URL with given endpoint.
        :param endpoint:
        :return: URL targeting to given endpoint
        """
        return self.baseURL + "/" + endpoint

    async def request(self, mode: str, endpoint: str,
                params = None, data = None, json = None):
        """
        <method AbstractHTTPConnection.request>
        Create requesting coroutine.
        :param mode:            Restful mode. One of the POST, GET, PUT, DELETE.
        :param endpoint:        Target endpoint.
        :param params:          Parameters for HTTP request.
        :param data:            Data for HTTP request.
        :param header:          Header for HTTP request.
        :return Request coroutine, after executing coroutine, result can be acquired by <return value>.text() etc.
        """


        # Data, Json arguments can not be passed at the same time
        if data is not None and json is not None:
            raise connection.errors.InvalidError("Data, Json arguments can not be passed at the same time")

        # Make asynchronous request coroutine
        targetURL = self.targetURL(endpoint)
        print(targetURL)
        result = None
        mode = mode.upper()
        if mode == "POST":      result = await self.session.post(targetURL, data = data, json = json)
        elif mode == "GET":     result = await self.session.get(targetURL, params = params, json = json)
        elif mode == "PUT":     result = await self.session.put(targetURL, data = data, json = json)
        elif mode == "DELETE":  result = await self.session.delete(targetURL)
        else:                   raise connection.errors.InvalidError("Invalid mode(%s) to request" % (mode,))
        print(await result.json(), result.status)
        # Check http status exceptions
        error = None
        if result.status in self.httpExceptions:
            error = self.httpExceptions[result.status]
            if error == 'ServiceNotAvailable':
                if re.search('(cloudflare|incapsula|overload|ddos)', await result.text(), flags=re.IGNORECASE):
                    error = 'DDoSProtection'
        if error:
            raise connection.errors.InvalidError("HTTP status exception : HTTP code %s error(%s)" % (result.status, error))
        
        return result


# ----------------------------------------------------------------------------------------------------------------------
# Testing


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    ahc = AbstractHTTPConnection('https://api.kraken.com')
    temp = ahc.request('get', '0/public/Assets', '', 0)
    result = loop.run_until_complete(temp)
    result_txt = loop.run_until_complete(result.json())
    print(result_txt)
