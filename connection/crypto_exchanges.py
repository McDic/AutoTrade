"""
<module AutoTrade.connection.crypto_exchanges>
Abstract binder for all ccxt exchange sessions.
The reason why I made this is to avoid IP ban due to exceed the API call limit.
"""

# ----------------------------------------------------------------------------------------------------------------------
# Libraries

# Standard libraries
import time
import queue

# External libraries
import ccxt.async_support as ccxt
from ccxt.async_support.base.exchange import Exchange as BaseExchange

# Custom libraries
from connection.base import AbstractConnection

# ----------------------------------------------------------------------------------------------------------------------
# CCXT binder

class CCXTConnection(AbstractConnection):
    """
    <class CCXTConnection>
    CCXT exchange binder.
    """

    def __init__(self, exchange: BaseExchange):
        super().__init__(connectionName = "CCXT Binder")