"""
<module AutoTrade.connection.raw_crawler.bitcoincharts> (Deprecated)
Raw crawler at bitcoincharts.com to gather 1 minute interval BTCUSD OHLCV data.
"""
# ----------------------------------------------------------------------------------------------------------------------
# Libraries

# Standard libraries
import time, datetime

# External libraries
from selenium.common.exceptions import WebDriverException
from bs4 import BeautifulSoup

# Custom libraries
from connection.raw_crawler.base import AbstractCrawler

# ----------------------------------------------------------------------------------------------------------------------
# Raw crawler

class BitcoinChartsCrawler(AbstractCrawler):
    """
    <class BitcoinChartsCrawler>
    Raw crawler at bitcoincharts.com to gather 1 minute interval BTCUSD OHLCV data.
    [Note] This class is deprecated because it seems the website denies automated crawling processes.
    """

    exchangeXpath = {
        "Bitfinex":     '//*[@id="m"]/optgroup[38]/option[16]',
        "Bitflyer":     '//*[@id="m"]/optgroup[38]/option[18]',
        "Bitstamp":     '//*[@id="m"]/optgroup[38]/option[22]',
        "Coinbase":     '//*[@id="m"]/optgroup[38]/option[33]',
        "Kraken":       '//*[@id="m"]/optgroup[38]/option[52]',
        "OKCoin":       '//*[@id="m"]/optgroup[38]/option[57]',
    }

    def __init__(self, chrome_driver_path = "chromedriver", minInteractionInterval = 0.1):

        # Parent class initialization
        super().__init__(connectionName = "bitcoincharts.com Crawler", chrome_driver_path = chrome_driver_path,
                         startURL = "https://bitcoincharts.com/charts")
        self.minInteractionInterval = minInteractionInterval

        # Base set
        self.driver.find_element_by_xpath('//*[@id="c"]').click() # Check 'Custom Time'
        self.driver.find_element_by_xpath('//*[@id="i"]/option[2]').click() # Select time period to 1 minute
        self.driver.find_element_by_xpath('//*[@id="t"]/option[5]').click() # Select chart type to OHLC
        self.dateInputStartBox = self.driver.find_element_by_xpath('//*[@id="s"]')
        self.dateInputEndBox = self.driver.find_element_by_xpath('//*[@id="e"]')
        self.drawButton = self.driver.find_element_by_xpath('//*[@id="SubmitButton"]')
        self.dataLoaderLink = self.driver.find_element_by_xpath('//*[@id="content_chart"]/div/div[2]/a')
        time.sleep(self.minInteractionInterval)

    def selectExchange(self, exchangeName: str):
        assert exchangeName in self.exchangeXpath
        self.driver.find_element_by_xpath(self.exchangeXpath[exchangeName]).click()

    def selectDate(self, date: datetime.datetime):
        y, m, d = date.year, date.month, date.day
        self.dateInputStartBox.clear()
        self.dateInputStartBox.send_keys("%04d-%02d-%02d" % (y, m, d))
        time.sleep(self.minInteractionInterval)
        self.dateInputEndBox.clear()
        self.dateInputEndBox.send_keys("%04d-%02d-%02d" % (y, m, d))
        time.sleep(self.minInteractionInterval)
        self.drawButton.click()
        time.sleep(5)
        a = input("ddd")
        self.dataLoaderLink.click()
        time.sleep(5)

    def getGraph(self):
        soup = BeautifulSoup(self.driver.page_source, "html.parser")
        table = soup.find("table", {"class": "data", "id": "chart_table"})
        rows = table.find("tbody").find_all("tr")
        data = {} # {timestamp: (O, H, L, C, V)}
        for row in rows:
            attributes = row.find_all("td")
            raise NotImplementedError


if __name__ == "__main__":
    with BitcoinChartsCrawler() as Crawler:
        Crawler.selectExchange("Bitfinex")
        Crawler.selectDate(datetime.datetime(2018, 1, 5))
        Crawler.getGraph()

