"""
<module AutoTrade.connection.base>
This module is used to describe the abstraction of all type of connection.
"""
# ----------------------------------------------------------------------------------------------------------------------
# Libraries

# Standard libraries
import time
import queue
from decimal import Decimal
from datetime import datetime, timedelta
import atexit
from sys import exc_info

# External libraries

# Custom libraries
import connection.errors as cerr
import utility

# ----------------------------------------------------------------------------------------------------------------------
# Abstract base

# All exceptions are default to catch in call rate limiter
_defaultCatching = frozenset([cerr.AutoTradeConnectionError,])

# ABC for Connection
class AbstractConnection:
    """
    <class AbstractConnection>
    Abstract base of all connection.
    """

    # ------------------------------------------------------------------------------------------------------------------
    # Base methods

    def __init__(self, connectionName: str = None, callLimits: dict = None):
        """
        <method AbstractConnection.__init__>
        :param connectionName:  Name of this connection.
        :param callLimits:      Define call limits. Syntax: {Field name: (Refreshing time interval(sec), Max weight)}
        """

        # Basic attributes
        self.name = connectionName if connectionName else "Unnamed_Connection_0x%X" % (id(self),)
        self.callLimits = {} # {field name: (refreshing time interval(sec), max weight)}

        # Call limits
        self.__catching_set = set(_defaultCatching) # Set of catching exceptions
        self.__catching_tuple = tuple(_defaultCatching) # Tuple of catching exceptions
        if isinstance(callLimits, dict):
            for callFieldName in callLimits:
                timeInterval, maxWeight = callLimits[callFieldName]
                self.addCallField(callFieldName, timeInterval, maxWeight)
        elif callLimits: raise cerr.InvalidError("Given call limits is not valid (type %s)" % (type(callLimits),))

        # Register termination at exit
        atexit.register(terminateSessionAtExit, self)

    def __str__(self): return "Abstract Connection [%s]" % (self.name,)

    # ------------------------------------------------------------------------------------------------------------------
    # Supporting with clause

    def __enter__(self): return self
    def __exit__(self, exc_type, exc_val, exc_tb): self.terminate()

    # ------------------------------------------------------------------------------------------------------------------
    # Termination

    def terminate(self): pass

    # ------------------------------------------------------------------------------------------------------------------
    # Call field related

    def addCallField(self, callFieldName: str, timeInterval: (int, float, Decimal), maxWeight: (int, float, Decimal)):
        """
        <method AbstractConnection.addCallField>
        Add new call limit field with given parameters.
        The call limit(a.k.a. self.callLimits[]) is json-like structure. The details are described below:
            - time_interval (numeric): Refreshing time interval in second.
            - max_weight (numeric): Maximum weight able to send in given interval
            - history (queue.PriorityQueue): PriorityQueue [(timestamp, weight), ...]
            - reserved_weight (numeric): Sum of all reserved weights. Added due to the failure of the process.
            - current_weight (numeric): Sum of all weights in current history.
            - oldest_timestamp (datetime.datetime): Oldest timestamp in history.
        :param callFieldName:   The name of call field.
        :param timeInterval:    Call history saving time in seconds.
        :param maxWeight:       Max call weight capacity for time interval.
        """

        # Validity checking
        if not isinstance(callFieldName, str): # Call field name is not string
            raise cerr.InvalidError("Given callFieldName type is not string(%s given)" % (type(callFieldName),))
        elif not callFieldName: # Empty string given
            raise cerr.InvalidError("Empty string cannot be call field name('%s' given)" % (callFieldName,))
        elif callFieldName in self.callLimits: #
            raise cerr.InvalidError("Already same callFieldName [%s] exist" % (callFieldName,))
        elif not (isinstance(maxWeight, (int, float, Decimal)) and maxWeight > 0):
            raise cerr.InvalidError("Given maxWeight argument is invalid (type %s, value %s)" % (type(maxWeight), maxWeight))
        elif not (isinstance(timeInterval, (int, float, Decimal)) and timeInterval > 0):
            raise cerr.InvalidError("Given timeInterval argument is invalid (type %s, value %s)" % (type(timeInterval), timeInterval))

        # Add new call field
        if isinstance(timeInterval, (int, float, Decimal)):
            timeInterval = timedelta(seconds = float(timeInterval))
        elif not isinstance(timeInterval, timedelta):
            raise cerr.InvalidError("Invalid timeInterval type(%s) given for call field" % (type(timeInterval)))
        self.callLimits[callFieldName] = {"time_interval": timeInterval, "max_weight": maxWeight,
                                          "current_weight": 0, "reserved_weight": 0,
                                          "oldest_timestamp": datetime.min, "history": queue.PriorityQueue()}

    def refreshCallField(self, callFieldName: str):
        """
        <method AbstractConnection.refreshCallField>
        Remove the old history of queries(older than current_time - time_interval) with given field name.
        :param callFieldName: The name of call field.
        """

        thisCallLimit = self.callLimits[callFieldName]
        now = datetime.now()
        while not thisCallLimit["history"].empty():
            timestamp, weight = thisCallLimit["history"].get()
            if now - thisCallLimit["time_interval"] < timestamp: # Got event which occurred in interval
                thisCallLimit["history"].put((timestamp, weight)) # Should not be deleted, put again
                thisCallLimit["oldest_timestamp"] = timestamp
                break
            thisCallLimit["current_weight"] -= weight
        else: thisCallLimit["oldest_timestamp"] = datetime.min # All event deleted

    def isPossibleCall(self, callFieldName: str, weight: (int, float, Decimal)):
        """
        <method AbstractConnection.isPossibleCall>
        Check if the new call with given field name and weight is possible now
        :param callFieldName:   The name of call field.
        :param weight:          The weight of call.
        :return Return if new call is possible or not.
        """

        # Validity checking and edge case controlling
        if not callFieldName: return True # Empty string or None always implies this call is possible.
        elif callFieldName not in self.callLimits: raise cerr.InvalidError("Given callFieldName '%s' is not exist" % (callFieldName,))
        elif weight == 0: return True # Even if the weight is zero, given call field name must be valid.
        elif weight < 0: raise cerr.InvalidError("Given weight(%s) is negative" % (weight,))

        # If total weight is not exceeded then the new call is possible
        thisCallLimit = self.callLimits[callFieldName]
        if thisCallLimit["oldest_time"] != datetime.min and datetime.now() - thisCallLimit["time_interval"] > thisCallLimit["oldest_timestamp"]:
            self.refreshCallField(callFieldName)
        return thisCallLimit["current_weight"] + thisCallLimit["reserved_weight"] + weight <= thisCallLimit["max_weight"]

    # ------------------------------------------------------------------------------------------------------------------
    # Call related; Making call

    def addExceptionsForToleration(self, *args):
        """
        <static method AbstractConnection.addExceptionsForToleration>
        Add given exceptions for AbstractConnection.__catching_tuple.
        All given exceptions are already in __catching_set / __catching_tuple will be ignored.
        """
        for givenError in args:
            if not isinstance(givenError, Exception): # Non-exception given
                self.__catching_tuple = tuple(self.__catching_set)
                raise cerr.InvalidError("Invalid type %s given; Not inherited from Exception." % (type(givenError),))
            elif givenError not in self.__catching_set:  # Append if only new exceptions are given
                self.__catching_set.add(givenError)
        self.__catching_tuple = tuple(self.__catching_set)

    def removeExceptionsForToleration(self, *args):
        """
        <static method AbstractConnection.removeExceptionsForToleration>
        Remove given exceptions for AbstractConnection.__catching_tuple.
        All given exceptions are not in __catching_set / __catching_tuple will be ignored.
        """
        for givenError in args:
            if not isinstance(givenError, Exception): # Non-exception given
                self.__catching_tuple = tuple(self.__catching_set)
                raise cerr.InvalidTypeError("Invalid type %s given; Not inherited from Exception." % (type(givenError),))
            elif givenError in _defaultCatching: # Tried to remove default catching exception
                self.__catching_tuple = tuple(self.__catching_set)
                raise cerr.InvalidError("Tried to remove default-catching Exceptions")
            elif givenError in self.__catching_set: # Remove only if already exist
                self.__catching_set.remove(givenError)
        self.__catching_tuple = tuple(self.__catching_set)

    def _makeCallSync(method):
        """
        <static method AbstractConnection._makeCallSync>
        Reserve the call weight before process.
        Given method should have 'callFieldName' and 'callWeight' in kwargs, otherwise the call will not affect by any limit field.
        To make call, just add @AbstractConnection._makeCallSync for method.
        Note that this function is not decorated by @staticmethod, but this method is static.
        :return: Decorated method.
        """
        def decorated(self: AbstractConnection, *args, **kwargs):

            # If parameter is not given
            if "callFieldName" not in kwargs: raise cerr.InvalidError("Call field name is not given")
            elif "callWeight" not in kwargs: kwargs["callWeight"] = 0 # If weight is not given then it's considered as 0

            # Attributes
            callFieldName = kwargs["callFieldName"]
            callWeight = kwargs["callWeight"]

            # Edge case control
            if not self.isPossibleCall(callFieldName, callWeight): # Internal rate limiting: Call limit exceeded.
                raise cerr.CallLimitExceededError(self.name, callFieldName)
            elif not callFieldName or callWeight == 0: # Always possible call don't need any additional processes.
                return method(self, *args, **kwargs)

            # Reserve weight, process, post-process, finally return or raise.
            # Concept of reserved weight is from handling concurrency.
            thisCallLimit = self.callLimits[callFieldName]
            thisCallLimit["reserved_weight"] += callWeight
            try: result = method(self, *args, **kwargs) # Main process
            except self.__catching_tuple as err: # Cancelled calling so there is no new call history
                thisCallLimit["reserved_weight"] -= callWeight
                raise err.with_traceback(exc_info()[2])
            except Exception as err: # Successfully called so put new call on history
                thisCallLimit["history"].put((datetime.now(), callWeight))
                thisCallLimit["current_weight"] += callWeight
                thisCallLimit["reserved_weight"] -= callWeight
                raise err.with_traceback(exc_info()[2])
            else: # Process completed
                thisCallLimit["history"].put((datetime.now(), callWeight))
                thisCallLimit["current_weight"] += callWeight
                thisCallLimit["reserved_weight"] -= callWeight
                return result
        return decorated

    def _makeCallAsync(method):
        """
        <static method AbstractConnection._makeCallAsync>
        Do same thing as AbstractConnection._makeCallSync, but this method is for coroutine.
        Note that this function is not decorated by @staticmethod, but this method is static.
        :return: Decorated coroutine.
        """
        async def decorated(self, *args, **kwargs):

            # If parameter is not given
            if "callFieldName" not in kwargs: raise cerr.InvalidError("Call field name is not given")
            elif "callWeight" not in kwargs: kwargs["callWeight"] = 0 # If weight is not given then it's considered as 0

            # Attributes
            callFieldName = kwargs["callFieldName"]
            callWeight = kwargs["callWeight"]

            # Edge case control
            if not self.isPossibleCall(callFieldName, callWeight): # Internal rate limiting: Call limit exceeded.
                raise cerr.CallLimitExceededError(self.name, callFieldName)
            elif not callFieldName or callWeight == 0: # Always possible call don't need any additional processes.
                return await method(self, *args, **kwargs)

            # Reserve weight, process, post-process, finally return or raise.
            # Concept of reserved weight is from handling concurrency.
            thisCallLimit = self.callLimits[callFieldName]
            thisCallLimit["reserved_weight"] += callWeight
            try: result = method(self, *args, **kwargs) # Main process
            except self.__catching_tuple as err: # Cancelled calling so there is no new call history
                thisCallLimit["reserved_weight"] -= callWeight
                raise err.with_traceback(exc_info()[2])
            except Exception as err: # Successfully called so put new call on history
                thisCallLimit["history"].put((datetime.now(), callWeight))
                thisCallLimit["current_weight"] += callWeight
                thisCallLimit["reserved_weight"] -= callWeight
                raise err.with_traceback(exc_info()[2])
            else: # Process completed
                thisCallLimit["history"].put((datetime.now(), callWeight))
                thisCallLimit["current_weight"] += callWeight
                thisCallLimit["reserved_weight"] -= callWeight
                return result
        return decorated

# ----------------------------------------------------------------------------------------------------------------------
# Auto termination at exit. It is not guaranteed to work in all situations; This works only for normal termination.
def terminateSessionAtExit(session: AbstractConnection):
    try: session.terminate()
    except Exception as err: print("[Warning] Exception <%s> occurred while terminating <%s>\n" % (err, session.name))

# ----------------------------------------------------------------------------------------------------------------------
# __all__