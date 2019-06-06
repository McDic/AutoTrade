"""
<module utility.__init__>
This module provides integrated utilities.
"""

# ----------------------------------------------------------------------------------------------------------------------
# Libraries

# Standard libraries
import sys
import time
import asyncio
from datetime import datetime

# External libraries

# Custom libraries

# ----------------------------------------------------------------------------------------------------------------------
# Print framing
def printFrame(inputLines, initmsg: str = "", endmsg: str = "", totalsize: int = 120, printing: bool = True):
    """
    <function printFrame>
    :param inputLines: Actual string data. All rightmost empty spaces will be pruned.
    :return: List of lines like
        +---- Begin ----+
        | ......        |
        | ...           |
        | ........      |
        +----- End -----+
    """

    # Validity checking
    if totalsize < 20 or not isinstance(totalsize, int):
        raise ValueError("Invalid or too small totalsize(%s) given" % (totalsize,))
    elif len(initmsg) > totalsize - 6 or len(endmsg) > totalsize - 6:
        raise ValueError("Too long initmsg(len = %d) or endmsg(len = %d)" % (len(initmsg), len(endmsg)))

    # Calculate length of bars making '+----- (title) -----+'
    initBarCountRight  = (totalsize - 4 - len(initmsg)) // 2
    initBarCountLeft = totalsize - 4 - len(initmsg) - initBarCountRight
    endBarCountRight  = (totalsize - 4 - len(endmsg)) // 2
    endBarCountLeft = totalsize - 4 - len(endmsg) - endBarCountRight

    # Constructing lines
    lines = ["+" + ("-" * initBarCountLeft + " " + initmsg + " " + "-" * initBarCountRight if initmsg else "-" * (totalsize-2)) + "+"]
    for line in inputLines:
        line = line.rstrip(" \t\n")
        if len(line) + 4 > totalsize: lines.append("| " + line)
        else: lines.append("| " + line + " " * (totalsize - len(line) - 4) + " |")
    lines.append("+" + ("-" * endBarCountLeft + " " + endmsg + " " + "-" * endBarCountRight if endmsg else "-" * (totalsize-2)) + "+")

    # Print result and return
    if printing: print("\n".join(lines))
    return lines

# ----------------------------------------------------------------------------------------------------------------------
# TimeMeasure: Measure time to estimate performances
class TimeMeasure:
    """
    <class TimeMeasure>
    Used to measure time interval between checkpoints.
    """

    # Initializer
    def __init__(self):
        self.lastUpdatedTime = time.time()

    # Update
    def update(self, printing = False):
        """
        <method TimeMeasure.update>
        Update checkpoints. If printing is True then print the debugged info.
        :return: Time interval between last 2 checkpoints.
        """
        nowTime = time.time()
        timeDiff = nowTime - self.lastUpdatedTime
        self.lastUpdatedTime = nowTime
        if printing: raise NotImplementedError
        return timeDiff

# ----------------------------------------------------------------------------------------------------------------------
# Analyzing: Analyze invocation of given function or method
def analyzer(method):
    """
    <function analyzer>
    Analyze and debug given arguments, execution time, result, etc.
    :return: Decorated method.
    """
    def analyzed_method(*args, **kwargs):
        debug_lines = ["Given arguments: args %s, kwargs %s" % (args, kwargs)]
        usedTime = - time.time() # Used time = end time - start time
        try: result = method(*args, **kwargs) # Try method call
        except BaseException as err: # If error occurred then print type of error and raise again.
            usedTime += time.time()
            debug_lines.append("Error %s occurred while performing (%.3f sec used)" % (type(err), usedTime))
            printFrame(debug_lines, "Analyzing callable <%s>" % (method.__qualname__,))
            sys.stdout.flush() # For clean output, flush and wait minimum interval
            time.sleep(0.01)
            raise err
        else: # If successfully executed then print result value and return.
            usedTime += time.time()
            debug_lines.append("Successfully executed, result is %s (%.3f sec used)" % (result, usedTime))
            printFrame(debug_lines, "Analyzing callable <%s>" % (method.__qualname__,),)
            return result
    return analyzed_method

def analyze(method, *args, **kwargs):
    """
    <function analyze>
    :return: Same as result of method(*args, **kwargs) but with analyzer.
    """
    return analyzer(method)(*args, **kwargs)

# ----------------------------------------------------------------------------------------------------------------------
# Async related
def concurrentResults(*coroutines, timeout = None):
    """
    <function concurrentResults>
    Run all given coroutines concurrently until all tasks are finished and return the result.
    Raised errors are tolerated and treated as result.
    :param coroutines: List of coroutines.
    :param timeout: Maximum timeout in seconds. None for infinite timeout.
    :return: List of result of coroutines.
    """
    eventLoop = asyncio.get_event_loop()
    tasks = [eventLoop.create_task(coro) for coro in coroutines]
    results = []
    assert timeout is None or (isinstance(timeout, (int, float)) and timeout > 0)
    for task in tasks:
        try: results.append(eventLoop.run_until_complete(asyncio.wait_for(task, timeout = timeout)))
        except Exception as err: results.append(err)
    return results

# ----------------------------------------------------------------------------------------------------------------------
# Timestamp container

class TimestampContainer(dict):
    """
    <class TimestampContainer> inherited from dict
    """

    def __getitem__(self, key: datetime):
        if isinstance(key, datetime): return super().__getitem__(round(key.timestamp()))
        else: raise TypeError("Key should be instance of datetime")
    def __setitem__(self, key: datetime, value):
        if isinstance(key, datetime): return super().__setitem__(round(key.timestamp()), value)
        else: raise TypeError("Key should be instance of datetime")
    def __delitem__(self, key: datetime):
        if isinstance(key, datetime): return super().__delitem__(round(key.timestamp()))
        else: raise TypeError("Key should be instance of datetime")

# ----------------------------------------------------------------------------------------------------------------------
# Functionality testing

if __name__ == "__main__":

    a = TimestampContainer()
    a[datetime(2019, 5, 30, 10, 0, 0)] = 1
    a[datetime(2019, 5, 29, 6, 40, 0)] = 2
    for key in range(a):
        print("key = %d" % (key,))
        val = a[key]
        print("Object[%s] = %s" % (key, val))

