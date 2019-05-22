"""
<module AutoTrade.simulator>
Simulator based on logic.
"""

# ----------------------------------------------------------------------------------------------------------------------
# Libraries

# Standard libraries
import time
from pprint import pprint

# External libraries
import numpy as np
import matplotlib.pyplot as plt

# Custom libraries

# ----------------------------------------------------------------------------------------------------------------------
# Simulator class
class Simulator:
    """
    <class Simulator>
    Historic data back-tester.
    """

    # ------------------------------------------------------------------------------------------------------------------
    # Constructor

    def __init__(self, name: str = "Simulator"):

        # Attributes
        self.name = name
        self.figure, self.axes = plt.subplots(2, 1)

        x = np.linspace(1,10,200)
        y = np.sin(x)
        self.axes[0].plot(x, y, 'ko', x, y, 'b')
        self.axes[0].set_ylabel("sin")
        self.axes[1].plot(x, y**2)
        self.axes[1].set_ylabel("sin^2")
        plt.show()

if __name__ == "__main__":

    a = Simulator()