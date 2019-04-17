"""
<module AutoTrade.connection.http.base>
This module is used to describe the abstraction of all type of HTTP connection.
"""
# ----------------------------------------------------------------------------------------------------------------------
# Libraries

# Standard libraries
import re
import atexit
import copy

# External libraries
import aiohttp
import asyncio

# Custom libraries
import connection.base, connection.errors
from connection.http.errors import *

# ----------------------------------------------------------------------------------------------------------------------
# Abstract base

class AbstractHTTPConnectionClass(connection.base.AbstractConnection):
    """
    <class AbstractHTTPConnection>
    Abstract base of all HTTP connections.
    """

    # ------------------------------------------------------------------------------------------------------------------
    # HTTP exceptions codes
    
    httpExceptions = {
        422 : ServiceError,
        418 : DDoSProtection,
        429 : DDoSProtection,
        404 : ServiceNotAvailable,
        409 : ServiceNotAvailable,
        500 : ServiceNotAvailable,
        501 : ServiceNotAvailable,
        502 : ServiceNotAvailable,
        520 : ServiceNotAvailable,
        521 : ServiceNotAvailable,
        522 : ServiceNotAvailable,
        525 : ServiceNotAvailable,
        526 : ServiceNotAvailable,
        400 : ServiceNotAvailable,
        403 : ServiceNotAvailable,
        405 : ServiceNotAvailable,
        503 : ServiceNotAvailable,
        530 : ServiceNotAvailable,
        408 : RequestTimeout,
        504 : RequestTimeout,
        401 : AuthenticationError,
        511 : AuthenticationError,
    }

    # ------------------------------------------------------------------------------------------------------------------
    # Initializing methods

    def __init__(self, baseURL : str, connectionName: str = "AbstractHTTPConnection",
                 callLimits = None, keys = None, timeout : int = None, hooks = None):
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


        # Initialize session to none
        self.session = None

        # hook check
        if hooks is None:
            self.hooks = []
        elif hasattr(hooks,'__call__'):
            self.hooks = [hooks]
        else:
            try:
                self.checkValidHookList(hooks)
            except (connection.errors.InvalidTypeError, connection.errors.InvalidError):
                raise
            self.hooks = hooks

        # Register terminate function at exit
        atexit.register(self.terminate)

    async def _init_async(self, headers = None, cookies = None):
        """
        <async method AbstractHTTPConnection._init_async>
        Async initializing function
        :param headers: parameter for ClientSession
        :param cookies: parameter for ClientSession
        :return:
        """

        # Initialize header if None
        headers = headers or {}
        # Evading crawling prevention
        if headers is None or 'User-Agent' not in headers:
            headers.update({'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Mobile Safari/537.36'})
        # Quicker network response
        headers.update({'Accept-Encoding': 'gzip, deflate'})

        if self.session is None:
            self.session = aiohttp.ClientSession(headers = headers, cookies = cookies, timeout = self.timeout)

    def __str__(self): return "Abstract HTTP Connection [%s]" % (self.name,)
    
    # ------------------------------------------------------------------------------------------------------------------
    # Termination

    def terminate(self):
        """
        <method AbstractHTTPConnection.terminate>
        sync function to terminate class
        :return:
        """
        asyncio.get_event_loop().run_until_complete(self._terminate_async())

    async def _terminate_async(self):
        """
        <async method AbstractHTTPConnection._terminate_async>
        async function to terminate class
        :return:
        """
        await self.session.close()

    # ------------------------------------------------------------------------------------------------------------------
    # Hooking part
    @staticmethod
    def checkValidHookList(hooks):
        """
        <static method AbstractHTTPConnection.checkValidHookList>
        Check whether given hook list is valid or not
        :param hooks:    given hook list
        :return:
        """
        if not isinstance(hooks, list):
            raise connection.errors.InvalidTypeError('hooks must be "list" of callable functions')
        else:
            if all([hasattr(h, '__call__') for h in hooks]):
                pass
            else:
                raise connection.errors.InvalidError('hooks must be list of "callable" functions')

    def concatenateHooks(self, funcHooks: list):
        """
        hooks concatenating function
        :param funcHooks:    function's individual hooks
        :return:
        """
        return copy.deepcopy(self.hooks) + funcHooks

    # ------------------------------------------------------------------------------------------------------------------
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
                params = None, data = None, json = None, hooks = None):
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
        result = None
        mode = mode.upper()
        if mode == "POST":      result = await self.session.post(targetURL, data = data, json = json)
        elif mode == "GET":     result = await self.session.get(targetURL, params = params, json = json)
        elif mode == "PUT":     result = await self.session.put(targetURL, data = data, json = json)
        elif mode == "DELETE":  result = await self.session.delete(targetURL)
        else:                   raise connection.errors.InvalidError("Invalid mode(%s) to request" % (mode,))

        # Hooking part
        if hooks is None:
            hooks = []
        elif hasattr(hooks, '__call__'):
            hooks = [hooks]
        else:
            try:
                self.checkValidHookList(hooks)
            except (HTTPConnectionError):
                raise
            hooks = hooks
        finalHook = self.concatenateHooks(hooks)
        


        # Check http status exceptions
        error = None
        if result.status in self.httpExceptions:
            error = self.httpExceptions[result.status]
            if isinstance(error,ServiceNotAvailable):
                if re.search('(cloudflare|incapsula|overload|ddos)', await result.text(), flags=re.IGNORECASE):
                    error = DDoSProtection
        if error:
            raise error
        
        return result

# ----------------------------------------------------------------------------------------------------------------------
# Binder of AbstractHTTPConnectionClass

async def AbstractHTTPConnection(baseURL : str, connectionName: str = "AbstractHTTPConnection",
                 callLimits = None, timeout : int = None, headers = None,
                 cookies = None, hooks = None):
    connection = AbstractHTTPConnectionClass(baseURL, connectionName = connectionName, callLimits = callLimits,
                                             timeout = timeout, hooks = hooks)
    await connection._init_async(headers = headers, cookies = cookies)
    return connection

# ----------------------------------------------------------------------------------------------------------------------
# Testing

def printA():
    print("A")

def printB():
    print("B")

def printC():
    print("C")

if __name__ == "__main__":
    async def run():
        hooks = [printA, printB]
        abc = await AbstractHTTPConnection('http://gilgil.net/asfefe',hooks = hooks)
        co = await abc.request('get','', hooks = [printC])
        print (await co.text())

    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())

