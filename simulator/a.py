import sys
import os

from PyQt5.QtCore import Qt, QDateTime
from PyQt5.QtWidgets import QApplication, QMainWindow, QMenu, QVBoxLayout, \
    QSizePolicy, QMessageBox, QWidget, QPushButton, QHBoxLayout, QLabel, QDateTimeEdit, \
    QComboBox, QTextEdit
from PyQt5.QtGui import QIcon


from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
import matplotlib.pyplot as plt

import random


class App(QWidget):
    def __init__(self):

        super().__init__()

        self.drawButton = QPushButton("DRAW Graph")
        self.drawButton.clicked.connect(self.btnDrawClicked)

        self.randomDrawButton = QPushButton("DRAW Random Graph")
        self.randomDrawButton.clicked.connect(self.btnRandomDrawClicked)

        self.clearButton = QPushButton("Clear Graph")
        self.clearButton.clicked.connect(self.btnClearClicked)

        self.fig = plt.Figure()
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
        itemList = ['USD', 'BTC', 'ETH']
        selectorLayout = QHBoxLayout()

        # Base selector
        self.baseSelector = QComboBox()
        selectorLayout.addWidget(QLabel('Base'))
        self.baseSelector.addItems(itemList)
        selectorLayout.addWidget(self.baseSelector)

        # Target selector
        self.targetSelector = QComboBox()
        selectorLayout.addWidget(QLabel('Target'))
        self.targetSelector.addItems(itemList)
        selectorLayout.addWidget(self.targetSelector)

        # Text edit
        self.textEdit = QTextEdit()
        self.textEdit.setAcceptRichText(False)

        # Controller Layout
        controlLayout = QVBoxLayout()
        controlLayout.addLayout(startingLayout)
        controlLayout.addLayout(endingLayout)
        controlLayout.addLayout(selectorLayout)
        controlLayout.addWidget(self.textEdit)
        controlLayout.addStretch(1)
        controlLayout.addWidget(self.randomDrawButton)
        controlLayout.addWidget(self.drawButton)
        controlLayout.addWidget(self.clearButton)

        self.layout = QHBoxLayout()
        self.layout.addLayout(plotLayout)
        self.layout.addLayout(controlLayout)

        self.ax = self.fig.add_subplot(1, 1, 1)
        self.ax.grid(True)

        self.setLayout(self.layout)
        self.setGeometry(200, 200, 1000, 600)

    def btnDrawClicked(self):
        startingTimeStamp = self.startingDateTimeEdit.dateTime().toTime_t()
        endingTimeStamp = self.endingDateTimeEdit.dateTime().toTime_t()
        baseCurrency = self.baseSelector.currentText()
        targetCurrency = self.targetSelector.currentText()
        textEditContent = self.textEdit.toPlainText()

        text = f'start {startingTimeStamp}\nend {endingTimeStamp}\nbase {baseCurrency}\ntarget {targetCurrency}\n' + \
            f'textEdit {textEditContent}'

        QMessageBox.about(self, 'Hi', text)

    def btnRandomDrawClicked(self):
        self.ax.grid(True)
        xx = list(range(100))
        yy = list(range(100))
        random.shuffle(yy)
        self.ax.plot(xx,yy)
        self.canvas.draw()

    def btnClearClicked(self):
        self.ax.cla()
        self.ax.grid(True)
        self.canvas.draw()


if __name__ == '__main__':
    # For High dpi support
    os.environ["QT_AUTO_SCREEN_SCALE_FACTOR"] = "1"
    app = QApplication(sys.argv)
    app.setAttribute(Qt.AA_EnableHighDpiScaling)
    ex = App()
    ex.show()
    sys.exit(app.exec_())
