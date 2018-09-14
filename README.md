# AutoTrade

Quantitative Analysis for Cryptocurrency Trading.

# Pre-requisites

- Python 3.7+
    - requests: HTTP
    - psycopg2: PostgreSQL python binder
    
- PostgreSQL 10

# To-Do

- Implementation(or load) of exchange APIs
    - Binance
    - Bitmex
    - Bytex

- Price data base
    - Create back-testing environment
    
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

Using PostgreSQL DB from AWS.
- DB name: AutoTrade