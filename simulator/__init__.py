"""
<module simuator.__init__>
Simulator based on data.
"""

# ----------------------------------------------------------------------------------------------------------------------
# Libraries

# Standard libraries
import typing
from datetime import datetime, timedelta
from decimal import Decimal

# Custom libraries
from connection.database.pricebase_async import PriceBase
from simulator.errors import *

# ----------------------------------------------------------------------------------------------------------------------
# Simulator class

class FakeAccount:
    """
    <class FakeAccount>
    Temporary fake account to store simulation statistics data.
    """

    # ------------------------------------------------------------------------------------------------------------------
    # Constructor

    def __init__(self, initBalances: dict, absoluteBaseCurrency: str = "KRW", name: str = None):
        """
        <method FakeAccount.__init__>
        :param initBalances: Initial balances by currency.
        :param absoluteBaseCurrency: Absolute base currency used when this account try to measure balance.
        """

        # Name of fake account
        if self.name is None: self.name = "fakeaccount_%d" % (round(datetime.now().timestamp()),)
        else: self.name = name

        # Balance/Fee initialization
        self.balance, self.usedFee = {}, {}
        if initBalances: self.balance = initBalances
        for currency in self.balance: self.usedFee[currency] = 0

        # Non-financial statistics
        self.tradeHistory = [] # [(timestamp, baseCurrency, targetCurrency, amount, ratio)]
        self.currentTimestamp = datetime.min

    def __str__(self): return "Fake account <%s>" % (self.name,)
    __repr__ = __str__

    # ------------------------------------------------------------------------------------------------------------------
    # Trade

    def trade(self, timestamp: datetime, baseCurrency: str, targetCurrency: str, amount: Decimal, ratio: Decimal):
        """
        <method FakeAccount>
        :param timestamp: Trading timestamp. It must not be lower than self.currentTimestamp.
        :param baseCurrency:
        :param targetCurrency:
        :return:
        """

        # Validation
        if self.currentTimestamp > timestamp:
            raise PastAccessError("Past time(%s) accessed while fake account's time is %s" % (timestamp, self.currentTimestamp))
        elif baseCurrency not in self.balance or self.balance[baseCurrency] < amount:
            raise NotEnoughBalance(baseCurrency, amount, 0 if baseCurrency not in self.balance else self.balance[baseCurrency])

    def __abs__(self):
        """
        <method FakeAccount.__abs__>
        :return: Total value converted in absolute base currency.
        """
        pass

class Simulator:
    """
    <class Simulator>
    Simulate given logic on data from database.
    """

    def __init__(self, priceBase: PriceBase):
        """
        <method Simulator.__init__>
        :param priceBase: Price database for simulator.
        """
        self.priceDB = priceBase

<<<<<<< HEAD
    def simulateTick(self, tickLambda: typing.Callable[[typing.Any], float],
                     baseCurrency: str, quoteCurrency: str, exchange: str,
                     startTimestamp: datetime, endTimestamp: datetime,
                     fakeAccount: FakeAccount = None):
        """
        <method Simulator.simulate>
        Simulate given lambda from start time to end time in given pair.
        This method will be removed after
        :param startTimestamp:
        :param endTimestamp:
        :param fakeAccount:
        :return:
        """
=======
        x = np.linspace(1,10,200)
        y = np.sin(x)
        self.axes[0].plot(x, y, 'ko', x, y, 'b')
        self.axes[0].set_ylabel("sin")
        self.axes[1].plot(x, y**2)
        self.axes[1].set_ylabel("sin^2")
        plt.show()
>>>>>>> 6a3a4001e4825582aa804f2c21dd8b850ff125af

        # Validation
        if startTimestamp > endTimestamp:
            raise PastAccessError("Start timestamp(%s) is later than End timestamp(%s)" % (startTimestamp, endTimestamp))

        if fakeAccount is None: fakeAccount = FakeAccount(absoluteBaseCurrency = baseCurrency)
