"""
<module AutoTrade.connection.raw_crawler.base>
Abstract base for all chrome selenium crawler.
"""
# ----------------------------------------------------------------------------------------------------------------------
# Libraries

# Standard libraries

# External libraries
from selenium import webdriver
from selenium.common.exceptions import WebDriverException

# Custom libraries
from connection.base import AbstractConnection

# ----------------------------------------------------------------------------------------------------------------------
# Raw crawler

class AbstractCrawler(AbstractConnection):
    """
    <class AbstractCrawler>
    Abstract base of all selenium crawlers.
    """

    def __init__(self, connectionName = "Abstract Crawler", chrome_driver_path = "chromedriver", startURL = ""):
        """
        <method AbstractCrawler.__init__>
        :param connectionName:      The name of this connection.
        :param chrome_driver_path:  The path of "chromedriver.exe".
        :param startURL:            Home URL for this crawler.
        """

        # Parent class initialization
        super().__init__(connectionName = connectionName) # No key and call limits needed

        # Optimize Chrome options.
        chromeOptions = webdriver.ChromeOptions()
        prefs = {'profile.managed_default_content_settings.images': 2}
        chromeOptions.add_experimental_option("prefs", prefs)
        self.driver = webdriver.Chrome(executable_path = chrome_driver_path, chrome_options = chromeOptions)
        self.driver.minimize_window()
        if startURL: self.driver.get(startURL)
        self.driver.implicitly_wait(1)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        super().__exit__(exc_type, exc_val, exc_tb) # Parent class __exit__
        self.driver.close() # Driver termination