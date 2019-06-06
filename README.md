# AutoTrade

Quantitative Analysis Toolkit.

# Pre-requisites

- Python 3.7+
    - requests: HTTP requests
        - requests-futures: Asynchronous HTTP requests
    - selenium: Powerful raw crawling
        - beautifulsoup4: HTML parser
    - ~~psycopg2: PostgreSQL Python binder~~
    - asyncpg: Asynchronous PostgreSQL Python binder
    - pony: Excellent ORM for database
    - ccxt: Cryptocurrency exchanges binder
    
- PostgreSQL 10+
    - pgadmin 4 *(additional)*: GUI for PostgreSQL
    
# Abstraction

- **Market**: (base, quote, exchange)
    
    *This structure is used to introduce markets.*
    - base: The symbol of base stock or currency. ex) USDT
    - quote: The symbol of target stock or currency. ex) BTC
    - exchange: The exchange of that trading pair. ex) Binance

- **PriceTick**: (market, timestamp, price, volume)

    *This structure is used to store market price tick data.*
    - market: Market.
    - timestamp: The timestamp of transaction.
    - price: The price of transaction.
    - volume: The volume of transaction.

- **OHLCV**: (market, interval, timestamp, open, close, high, low, volume)
    
    *This structure is used to store market price OHLCV data.*
    - market: Market.
    - interval: The length of period, stored using **datetime.timedelta**.
    - timestamp: The timestamp of starting time of period unit.
    - open, close, high, low, volume: OHLCV data, stored using **Decimal(24,8)**.

# Database

Using PostgreSQL DB from localhost or AWS.

- **Table structure**: 

    Constructed one table per market due to the optimization.

    Table name = PriceData_(*exchange*)_ (*base*)_ (*quote*)_ (*minuteInterval*) mins
    (ex: *PriceData_Bitstamp_USD_BTC_1mins*)

    timestamp | open | high | low | close | volume
    ---- | ---- | ---- | ---- | ---- | ----
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
        
 * Example function
 ```
 def smallf(timestamp: datetime, i: int): return round((timestamp - i * timedelta(minutes = 1)).timestamp())
if len(price_data) < 5:
    return None, False, False
else:
    recentAverage = statistics.mean(price_data[smallf(timestamp, i)][criteria] for i in range(50)
                                    if smallf(timestamp, i) in price_data)
    if smallf(timestamp, 0) not in price_data: return recentAverage, False, False
    nowPrice = price_data[smallf(timestamp, 0)][criteria]
    return recentAverage, recentAverage > nowPrice, recentAverage < nowPrice
 ```