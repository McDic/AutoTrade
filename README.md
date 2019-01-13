# AutoTrade

Quantitative Analysis for Cryptocurrency Trading.

# Pre-requisites

- Python 3.7+
    - requests: HTTP requests
        - requests-futures: Asynchronous HTTP requests
    - selenium: Powerful raw crawling
        - beautifulsoup4: HTML parser
    - psycopg2: PostgreSQL Python binder
    - *(UNUSED) ccxt: Cryptocurrency exchanges binder*
    - *(UNUSED) boto3: AWS SDK for Python 3*
    
- PostgreSQL 10+
    - pgadmin 4: GUI for PostgreSQL
    - Amazon RDS: To operate PostgreSQL 24/7 and get much more advanced support

- Google Chrome: Used for crawling via selenium

# To-Do

- Implementation(or load) of exchange APIs
    - Binance

- Price data base
    - Source: Prefer 1 minute interval data.
        - [CryptoCompare](https://www.cryptocompare.com): Recent data source
        - [BitcoinChart](https://bitcoincharts.com/charts/bitstampUSD#rg1zig1-minzczsg2014-12-17zeg2014-12-18ztgSzm1g10zm2g25zv):
            Historical BTC minute data; Use selenium to crawl directly
    - Create back-testing environment
        - Using PostgreSQL with localhost or AWS RDS
        - Base template is OHLCV
    
# Abstraction

- **Market**: (baseCurrency, targetCurrency, exchange) -> json
    - *This structure is used to introduce markets.*
    - baseCurrency: The symbol of base stock or currency.
    - targetCurrency: The symbol of target stock or currency.
    - exchange: The exchange of that trading pair.

- **Price**: (market, timestamp, timeInterval, open, close, high, low, volume) -> DB
    - *This structure is used to store market price data.*
    - market: Market.
    - timestamp: The timestamp of starting time of period.
    - timeInterval: The length of period.
    - open, close, high, low: OHLC.
    - volume: The volume of one period in baseCurrency.
    
- **SmallSession**: (market, startedPrice, amount) -> json
    - *This structure is used to introduce 1-to-1 session.*
    *Partial or additional buying is not possible for this class.*
    - market: Market.
    - startedPrice: Bought price.
    - amount: Amount of stock or currency.

- **BigSession**: (market, SmallSessions[], )

- **Account**: (exchange, )

- **Model**: WIP

# Database

Using PostgreSQL DB from localhost or AWS.