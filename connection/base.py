"""
<module AutoTrade.connection.base>
This module is used to describe the abstraction of all type of connection.
"""
# ----------------------------------------------------------------------------------------------------------------------
# Libraries

# Standard libraries
import time
import queue

# External libraries

# Custom libraries
from connection.errors import *

# ----------------------------------------------------------------------------------------------------------------------
# Abstract base

class AbstractConnection:
    """
    <class AbstractConnection>
    Abstract base of all connection.
    """

    # ------------------------------------------------------------------------------------------------------------------
    # Base methods

    def __init__(self, connectionName: str = "AbstractConnection", callLimits: dict = None, key: dict = None):
        """
        <method AbstractConnection.__init__>
        :param connectionName:  Name of this connection.
        :param callLimits:      Define call limits. Syntax: {Field name: (Refreshing time interval(sec), Max weight)}
        :param key:             Define keys(passwords) to authenticate.
        """

        # Basic attributes
        self.name = connectionName
        self.callLimits = {} # {field name: (refreshing time interval(sec), max weight)}
        self.key = {} # {name: value (string)}

        # Adding call limits and key
        if type(callLimits) is dict:
            for callFieldName in callLimits:
                timeInterval, maxWeight = callLimits[callFieldName]
                self.addCallField(callFieldName, timeInterval, maxWeight)
        else: assert callLimits is None
        if type(key) is dict:
            for name in key: self.key[name] = str(key[name])
        else: assert key is None

    def __str__(self):
        return "Connection [%s]" % (self.name,)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        del self.key

    # ------------------------------------------------------------------------------------------------------------------
    # Call field related

    def addCallField(self, callFieldName: str, timeInterval: float, maxWeight: int):
        """
        <method AbstractConnection.addCallField>
        Add new call limit field with given parameters.
        :param callFieldName:   The name of call field.
        :param timeInterval:    Call history saving time in seconds.
        :param maxWeight:       Max call weight capacity for time interval.
        """

        # Validity checking
        if callFieldName in self.callLimits:
            raise InvalidError("Already same callFieldName [%s] exist" % (callFieldName,))
        assert type(maxWeight) in (int, float) and maxWeight > 0
        assert type(timeInterval) in (int, float) and timeInterval > 0

        # Add new call field
        self.callLimits[callFieldName] = {"time_interval": timeInterval, "max_weight": maxWeight,
                                          "history": queue.PriorityQueue(), "current_weight": 0,
                                          "oldest_timestamp": -1}
        # time_interval: Refreshing time interval
        # max_weight: Maximum weight able to send in given interval
        # history: PriorityQueue [(timestamp, weight), ...]
        # current_weight: Current cumulative weight
        # oldest_timestamp: Oldest timestamp in history

    def refreshCallField(self, callFieldName: str):
        """
        <method AbstractConnection.refreshCallField>
        Remove the old history of queries(older than current_time - time_interval) with given field name.
        :param callFieldName: The name of call field.
        """

        thisCallLimit = self.callLimits[callFieldName]
        while not thisCallLimit["history"].empty():
            timestamp, weight = thisCallLimit["history"].get()
            if time.time() - thisCallLimit["time_interval"] < timestamp: # Got recent event, should not be deleted
                thisCallLimit["history"].put((timestamp, weight))
                thisCallLimit["oldest_timestamp"] = timestamp
                break
            thisCallLimit["current_weight"] -= weight
        else: # No event in history
            thisCallLimit["oldest_timestamp"] = -1

    def isPossibleCall(self, callFieldName: str, weight: int):
        """
        <method AbstractConnection.isPossibleCall>
        Check if the new call with given field name and weight is possible now
        :param callFieldName:   The name of call field.
        :param weight:          The weight of call.
        :return Return if new call is possible or not.
        """

        # If no call field name then always return True
        if callFieldName is None: return True

        # Validity checking
        assert callFieldName in self.callLimits
        assert type(weight) is int and weight > 0

        # If total weight is not exceeded then the new call is possible
        thisCallLimit = self.callLimits[callFieldName]
        if time.time() - thisCallLimit["time_interval"] > thisCallLimit["oldest_timestamp"]:
            self.refreshCallField(callFieldName)
        return thisCallLimit["current_weight"] + weight <= thisCallLimit["max_weight"]

    # ------------------------------------------------------------------------------------------------------------------
    # Handle responses

    def responseHandle(self, req, mode: str):
        """
        <method AbstractConnection.responseHandle>
        :param req: Request to handle.
        :param mode: Handling mode.
        :return: Processed data from response.
        """
        raise NotImplementedError("%s is not ready to handle any responses" % (self.name,))

# ----------------------------------------------------------------------------------------------------------------------
# __all__