"""
<module utility.timerelated>
This module is giving some functionalities related to time, date, timezone, etc.
"""

# ----------------------------------------------------------------------------------------------------------------------
# Libraries

# Standard libraries
import time

# Custom libraries

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
        nowTime = time.time()
        timeDiff = nowTime - self.lastUpdatedTime
        self.lastUpdatedTime = nowTime
        if printing:
            # print("[DEBUG] ")
            raise NotImplementedError
        return timeDiff
