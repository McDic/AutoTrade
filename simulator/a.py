# ----------------------------------------------------------------------------------------------------------------------
# Libraries

# Standard libraries
import asyncio
import statistics
from pprint import pprint
import typing
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import sys
import os
import random

from PyQt5.QtCore import Qt, QDateTime
from PyQt5.QtWidgets import QApplication, QMainWindow, QMenu, QVBoxLayout, \
    QSizePolicy, QMessageBox, QWidget, QPushButton, QHBoxLayout, QLabel, QDateTimeEdit, \
    QComboBox, QTextEdit

from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
import matplotlib.pyplot as plt

# Custom libraries
from connection.database.pricebase_async import PriceBaseClass, PriceBase
from simulator.errors import *
from simulator.naive import NaiveSimulator, smallf


class App(QWidget):
    def __init__(self, NSR):

        super().__init__()

        self.NSR = NSR

        self.drawButton = QPushButton("DRAW Graph")
        self.drawButton.clicked.connect(self.btnDrawClicked)

        self.clearButton = QPushButton("Clear Graph")
        self.clearButton.clicked.connect(self.btnClearClicked)

        self.fig, self.ax1 = plt.subplots()

        self.canvas = FigureCanvas(self.fig)

        self.startingDateTimeEdit = QDateTimeEdit(self)
        self.startingDateTimeEdit.setDateTime(QDateTime.currentDateTime())

        self.endingDateTimeEdit = QDateTimeEdit(self)
        self.endingDateTimeEdit.setDateTime(QDateTime.currentDateTime())

        # plotting part layout
        plotLayout = QVBoxLayout()
        plotLayout.addWidget(self.canvas)

        # Starting timestamp controller
        startingLayout = QHBoxLayout()
        startingLayout.addWidget(QLabel('Starts at'))
        startingLayout.addWidget(self.startingDateTimeEdit)

        # Ending timestamp controller
        endingLayout = QHBoxLayout()
        endingLayout.addWidget(QLabel('Ends at'))
        endingLayout.addWidget(self.endingDateTimeEdit)

        # Selector
        baseitemList = ['USD', 'EUR', 'JPY']
        targetitemList = ['BTC']
        selectorLayout = QHBoxLayout()

        # Base selector
        self.baseSelector = QComboBox()
        selectorLayout.addWidget(QLabel('Base'))
        self.baseSelector.addItems(baseitemList)
        selectorLayout.addWidget(self.baseSelector)

        # Target selector
        self.targetSelector = QComboBox()
        selectorLayout.addWidget(QLabel('Target'))
        self.targetSelector.addItems(targetitemList)
        selectorLayout.addWidget(self.targetSelector)

        # Exchange selector
        exchangeLayout = QHBoxLayout()
        exchangeLayout.addWidget(QLabel("Exchange"))
        self.exchangeSelector = QComboBox()
        self.exchangeSelector.addItems(['Bitfinex', 'Bitflyer', 'Bitstamp', 'Coinbase', 'Kraken', 'Mtgox', 'Okcoin', 'Zaif'])
        exchangeLayout.addWidget(self.exchangeSelector)

        # Text edit
        self.textEdit = QTextEdit()
        self.textEdit.setAcceptRichText(False)

        # Controller Layout
        controlLayout = QVBoxLayout()
        controlLayout.addLayout(startingLayout)
        controlLayout.addLayout(endingLayout)
        controlLayout.addLayout(selectorLayout)
        controlLayout.addLayout(exchangeLayout)
        controlLayout.addWidget(self.textEdit)
        controlLayout.addStretch(1)
        controlLayout.addWidget(self.drawButton)
        controlLayout.addWidget(self.clearButton)

        self.layout = QHBoxLayout()
        self.layout.addLayout(plotLayout)
        self.layout.addLayout(controlLayout)


        self.setLayout(self.layout)
        self.setGeometry(200, 200, 1100, 600)

    def convertDecimalToFloat(self,result):
        for key in result:
            data = result[key]
            newData = list(data)
            if data[0]:
                newData[0] = float(data[0])
            if data[3]:
                newData[3] = float(data[3])
            result[key] = newData
        return result

    def btnDrawClicked(self):
        startingTimeStamp = self.startingDateTimeEdit.dateTime().toPyDateTime().replace(microsecond=0)
        endingTimeStamp = self.endingDateTimeEdit.dateTime().toPyDateTime().replace(microsecond=0)
        baseCurrency = self.baseSelector.currentText()
        targetCurrency = self.targetSelector.currentText()
        exchange = self.exchangeSelector.currentText()
        # textEditContent = self.textEdit.toPlainText()

        # text = f'start {startingTimeStamp}\nend {endingTimeStamp}\nbase {baseCurrency}\ntarget {targetCurrency}\n' + \
        #     f'exchange {exchange}\ntextEdit {textEditContent}'

        def formula(timestamp: datetime, price_data: dict, criteria=3):
            if len(price_data) < 5:
                return None, False, False
            else:
                recentAverage = statistics.mean(price_data[smallf(timestamp, i)][criteria] for i in range(50)
                                                if smallf(timestamp, i) in price_data)
                if smallf(timestamp, 0) not in price_data: return recentAverage, False, False
                nowPrice = price_data[smallf(timestamp, 0)][criteria]
                return recentAverage, recentAverage > nowPrice, recentAverage < nowPrice


        result = asyncio.get_event_loop().run_until_complete(
            NSR.simulate(formula, baseCurrency, targetCurrency, exchange, startingTimeStamp, endingTimeStamp))

        result = self.convertDecimalToFloat(result)

        x = sorted(result)
        y1 = [result[xx][0] for xx in x]
        y2 = [result[xx][-1] for xx in x]

        if hasattr(self, 'ax1'):
            self.ax1.cla()
        if hasattr(self, 'ax2'):
            self.ax2.cla()

        color = 'tab:red'
        self.ax1.plot(x,y1, color = color)
        self.ax1.tick_params(axis='y', labelcolor=color)

        self.ax2 = self.ax1.twinx()

        color = 'tab:blue'
        self.ax2.plot(x,y2, color = color)
        self.ax2.tick_params(axis='y', labelcolor=color)

        self.fig.tight_layout()

        self.canvas.draw()


    def btnClearClicked(self):
        self.ax1.cla()
        self.ax2.cla()
        self.canvas.draw()


if __name__ == '__main__':

    DB = asyncio.get_event_loop().run_until_complete(PriceBase(fileName="awsdb.authkey"))
    NSR = NaiveSimulator(DB)





    # result = {datetime(2016,1,2):(1,0,0,1),
    #           datetime(2016,1,3):(3,0,0,2),
    #           datetime(2016,1,4):(2,0,0,3),
    #           datetime(2016,1,5):(5,0,0,4),
    #           datetime(2016,1,6):(4,0,0,5),}
    # For High dpi support
    os.environ["QT_AUTO_SCREEN_SCALE_FACTOR"] = "1"
    app = QApplication(sys.argv)
    app.setAttribute(Qt.AA_EnableHighDpiScaling)
    ex = App(NSR)
    ex.show()
    sys.exit(app.exec_())
