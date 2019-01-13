# This file is used to represent custom constants.

# ----------------------------------------------------------------------------------------------------------------------
# Libraries

# Standard libraries
import time
# from enum import Enum # enumeration

# Custom libraries

# ----------------------------------------------------------------------------------------------------------------------
# TimeUnit: Enumeration for time unit
class __timeUnit:

    # Attributes
    second = 1
    minute = 60
    hour = 60*60
    day = 24*60*60

    # Contains
    def __contains__(self, item):
        return item in (self.second, self.minute, self.hour, self.day)

# Register object
timeUnit = __timeUnit()

# ----------------------------------------------------------------------------------------------------------------------
# Exchanges: Enumeration for exchanges
class __exchanges:

    # Attributes
    Binance = "Binance"
    Bittrex = "Bittrex"

# ----------------------------------------------------------------------------------------------------------------------
# TimeMeasure: Measure time to estimate performances
class TimeMeasure:

    # Initializer
    def __init__(self):
        self.lastUpdatedTime = time.time()

    # Update
    def update(self, printing = False):
        nowTime = time.time()
        timeDiff = nowTime - self.lastUpdatedTime
        self.lastUpdatedTime = nowTime
        if printing:
            print("[DEBUG] ")
            raise NotImplementedError
        return timeDiff
