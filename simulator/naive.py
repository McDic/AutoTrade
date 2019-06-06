"""
<module simuator.naive>
Naive simulator for KU project only.
"""

# ----------------------------------------------------------------------------------------------------------------------
# Libraries

# Standard libraries
import asyncio
import statistics
import typing
from datetime import datetime, timedelta, timezone
from decimal import Decimal

# Custom libraries
from connection.database.pricebase_async import PriceBaseClass, PriceBase
from simulator.errors import *

# ----------------------------------------------------------------------------------------------------------------------
# Naive simulator

def smallf(timestamp: datetime, i: int): return round((timestamp - i * timedelta(minutes = 1)).timestamp())

class NaiveSimulator:
    """
    <class NaiveSimulator>
    Naive simulator.
    """

    def __init__(self, database: PriceBaseClass):
        """
        <method NaiveSimulator.__init__>
        :param database:
        """
        self.DB = database

    async def simulate(self, formula: typing.Callable[[datetime, typing.Dict], typing.Any],
                       base: str, quote: str, exchange: str,
                       startTimestamp: datetime, endTimestamp: datetime,
                       priceCriteria = 3):
        """
        <method NaiveSimulator.simulate>
        Simulate the formula for given pair and time period.
        Warning - this method is very naive! I will delete this method after KU demonstration.
        :param formula: formula(datetime, price data) => (value, shouldBuy, shouldSell)
        :return: {timestamp: (formula result, cumulative profit)}
        """

        # Validation

        # Base construct
        result = {}
        currentTime = startTimestamp
        if currentTime.second == 0 and currentTime.microsecond == 0: pass
        else:
            currentTime -= timedelta(seconds = currentTime.second, microseconds = currentTime.microsecond)
            currentTime += timedelta(minutes = 1)
        quantumTime = timedelta(minutes = 1)

        # Looping
        currentMoney, isCurrentBase, lastBoughtPrice = Decimal(1), True, None
        pricedata, pricetasks = {}, {}
        while currentTime < endTimestamp:

            # print("Now simulating <%s>, current data %s" % (currentTime, pricedata))

            # Remove previous data and pull current data
            previousLimit = currentTime - 10 * quantumTime
            if previousLimit in pricedata: del pricedata[previousLimit]
            thisresult = await self.DB.select(exchange, base, quote, quantumTime, currentTime, limit = 1)
            for ts in thisresult: pricedata[round(ts.timestamp())] = thisresult[ts]

            # Get the result
            formulaValue, shouldBuy, shouldSell = formula(currentTime, pricedata)
            nowBought, nowSold = False, False
            if isCurrentBase and shouldBuy: # Buy
                isCurrentBase = False
                lastBoughtPrice = pricedata[smallf(currentTime, 0)][priceCriteria]
                nowBought = True
            elif not isCurrentBase and shouldSell: # Sell
                isCurrentBase = True
                currentMoney *= pricedata[smallf(currentTime, 0)][priceCriteria] / lastBoughtPrice
                nowSold = True

            # Result appending
            result[currentTime] = (formulaValue, nowBought, nowSold, currentMoney)
            #result[currentTime] = (formulaValue, shouldBuy, shouldSell, currentMoney)
            currentTime += quantumTime

        # Return
        return result


if __name__ == "__main__":

    DB = asyncio.get_event_loop().run_until_complete(PriceBase(fileName = "awsdb.authkey"))
    NSR = NaiveSimulator(DB)

    #print(asyncio.get_event_loop().run_until_complete(
    #              DB.select("Bitfinex", "USD", "BTC", timedelta(minutes = 1),
    #              beginTime = datetime(2016, 12, 22), limit = 1)))
    #print("=" * 120)

    def formula(timestamp: datetime, price_data: dict, criteria = 3):
        if len(price_data) < 5: return None, False, False
        else:
            recentAverage = statistics.mean(price_data[smallf(timestamp, i)][criteria] for i in range(50)
                                            if smallf(timestamp, i) in price_data)
            if smallf(timestamp, 0) not in price_data: return recentAverage, False, False
            nowPrice = price_data[smallf(timestamp, 0)][criteria]
            return recentAverage, recentAverage > nowPrice, recentAverage < nowPrice

    result = asyncio.get_event_loop().run_until_complete(
        NSR.simulate(formula, "USD", "BTC", "Bitfinex", datetime(2016, 12, 1), datetime(2016, 12, 1, hour = 1)))
    for timestamp in sorted(result):
        print("<%s> : %s" % (timestamp, result[timestamp]))