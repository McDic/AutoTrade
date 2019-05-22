import sys
import os

from PyQt5.QtCore import Qt, QDateTime
from PyQt5.QtWidgets import QApplication, QMainWindow, QMenu, QVBoxLayout, \
    QSizePolicy, QMessageBox, QWidget, QPushButton, QHBoxLayout, QLabel, QDateTimeEdit
from PyQt5.QtGui import QIcon


from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
import matplotlib.pyplot as plt

import random

class App(QWidget):
    def __init__(self):
        super().__init__()
        self.initUI()

        self.setLayout(self.layout)
        self.setGeometry(200, 200, 800, 400)

    def initUI(self):

        self.drawButton = QPushButton("DRAW Graph")
        self.drawButton.clicked.connect(self.btnDrawClicked)

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
        startingLayout.addWidget(QLabel('Start'))
        startingLayout.addWidget(self.startingDateTimeEdit)

        # Ending timestamp controller
        endingLayout = QHBoxLayout()
        endingLayout.addWidget(QLabel('End'))
        endingLayout.addWidget(self.endingDateTimeEdit)

        # controller Layout
        controlLayout = QVBoxLayout()
        controlLayout.addLayout(startingLayout)
        controlLayout.addLayout(endingLayout)
        controlLayout.addWidget(self.drawButton)
        controlLayout.addStretch(1)
        controlLayout.addWidget(self.clearButton)
        controlLayout.addStretch(10)

        self.layout = QHBoxLayout()
        self.layout.addLayout(plotLayout)
        self.layout.addLayout(controlLayout)

        self.ax = self.fig.add_subplot(1,1,1)
        self.ax.grid(True)


    def btnDrawClicked(self):
        QMessageBox.about(self,'Hi','start : {}'.format(self.startingDateTimeEdit.dateTime().toTime_t()))
        self.ax.grid(True)
        xx = list(range(100))
        yy = list(range(100))
        random.shuffle(yy)
        self.ax.plot(xx,yy)
        self.canvas.draw()

    def btnClearClicked(self):
        print('aa')
        self.ax.cla()
        self.canvas.draw()



if __name__ == '__main__':
    #For Hidpi support
    os.environ["QT_AUTO_SCREEN_SCALE_FACTOR"] = "1"
    app = QApplication(sys.argv)
    app.setAttribute(Qt.AA_EnableHighDpiScaling)
    ex = App()
    ex.show()
    sys.exit(app.exec_())
