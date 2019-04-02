# AutoTrade

Quantitative Analysis Toolkit.

# Pre-requisites

- Python 3.7+
    - requests: HTTP requests
        - requests-futures: Asynchronous HTTP requests
    - selenium: Powerful raw crawling
        - beautifulsoup4: HTML parser
    - ~~psycopg2~~ asyncpg: Asynchronous PostgreSQL Python binder
    - pony: Excellent ORM for database
    - ccxt: Cryptocurrency exchanges binder
    - ~~boto3: AWS SDK for Python 3~~
    
- PostgreSQL 10+
    - pgadmin 4: GUI for PostgreSQL
    - Amazon RDS: To operate PostgreSQL 24/7 and get much more advanced support

- Google Chrome: Used for crawling via selenium

# To-Do

- Implementation(or load) of exchange APIs
    - Use ccxt.async_support

- Price data base
    - Source: Prefer 1 minute interval data.
        - [CryptoCompare](https://min-api.cryptocompare.com/): Recent data source
        - [BitcoinCharts](https://bitcoincharts.com/charts/bitstampUSD#rg1zig1-minzczsg2014-12-17zeg2014-12-18ztgSzm1g10zm2g25zv):
            Historical BTC minute data; Use selenium to crawl directly
    - Create back-testing environment
        - Using PostgreSQL with localhost or AWS RDS
        - Base template is OHLCV
    
# Abstraction

- **Market**: (baseCurrency, targetCurrency, exchange)
    - *This structure is used to introduce markets.*
    - base: The symbol of base stock or currency.
    - quote: The symbol of target stock or currency.
    - exchange: The exchange of that trading pair.

- **Price**: (market, minuteInterval, timestamp, open, close, high, low, volume)
    - *This structure is used to store market price data.*
    - market: Market.
    - minuteInterval: The length of period described in minute.
    - timestamp: The timestamp of starting time of period unit.
    - open, close, high, low, volume: OHLCV using **Decimal(24,8)**.

# Database

Using PostgreSQL DB from localhost or AWS.

- **Table structure**: 

    Constructed one table per market due to the optimization.

    Table name = PriceData_(*exchange*)_(*base*)_(*quote*)_(*minuteInterval*) mins
    (ex: *PriceData_Bitstamp_USD_BTC_1mins*)

    timestamp | open | high | low | close | volume
    --- | --- | --- | --- | --- | ---
    2017-12-24 13:00:00+00 | 13019.82 | 13019.82 | 13019.82 | 13019.82 | 0.05
    2017-12-24 13:01:00+00 | 13098.09 | 13098.09 | 13098.09 | 13098.09 | 0.96350729
    2017-12-24 13:02:00+00 | 13148.11 | 13148.11 | 13148.11 | 13148.11 |	0.07149971
    2017-12-24 13:03:00+00 | 13140	 | 13140 | 13140 | 13140 | 0.26503305

    
    Note that all columns has constraint `NOT NULL`.
    - timestamp: The timestamp of starting time of the period, using *TIMESTAMPTZ*. 
        This column is the primary key, and has additional constraint `CHECK(timestamp <= NOW())`.
    - open: Opening price of the period, using `NUMERIC(24, 8)`.
    - high: Highest price of the period, using same type as open.
    - low: Lowest price of the period, using same type as open.
    - close: Closing price of the period, using same type as open.
    - volume: Trading volume of the period, using same type as open.
        This column has additional constraint `CHECK(volume > 0)`.
        This means there is no data for periods with no trading volume.