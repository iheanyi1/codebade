import ccxt
import pandas as pd
import time
import requests
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

# Set up SQLite database
engine = sqlalchemy.create_engine('sqlite:///crypto.db')
Base = sqlalchemy.orm.declarative_base()
Session = sessionmaker(bind=engine)
session = Session()

# Define database tables
class Exchange(Base):
    __tablename__ = 'exchange'
    exchange_id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    name = sqlalchemy.Column(sqlalchemy.String)
    country = sqlalchemy.Column(sqlalchemy.String)
    website = sqlalchemy.Column(sqlalchemy.String)

class Token(Base):
    __tablename__ = 'token'
    token_id = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
    symbol = sqlalchemy.Column(sqlalchemy.String)
    name = sqlalchemy.Column(sqlalchemy.String)

class Price(Base):
    __tablename__ = 'price'
    price_id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    token_id = sqlalchemy.Column(sqlalchemy.String, sqlalchemy.ForeignKey('token.token_id'))
    exchange_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('exchange.exchange_id'))
    timestamp = sqlalchemy.Column(sqlalchemy.DateTime, default=datetime.utcnow)
    price = sqlalchemy.Column(sqlalchemy.Float)

class Volume(Base):
    __tablename__ = 'volume'
    volume_id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    token_id = sqlalchemy.Column(sqlalchemy.String, sqlalchemy.ForeignKey('token.token_id'))
    exchange_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('exchange.exchange_id'))
    timestamp = sqlalchemy.Column(sqlalchemy.DateTime, default=datetime.utcnow)
    volume = sqlalchemy.Column(sqlalchemy.Float)

class MarketCap(Base):
    __tablename__ = 'MarketCap'
    MarketCap_id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    token_id = sqlalchemy.Column(sqlalchemy.String, sqlalchemy.ForeignKey('token.token_id'))
    exchange_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('exchange.exchange_id'))
    timestamp = sqlalchemy.Column(sqlalchemy.DateTime, default=datetime.utcnow)
    MarketCap = sqlalchemy.Column(sqlalchemy.Float)

class TVL(Base):
    __tablename__ = 'TVL'
    TVL_id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    token_id = sqlalchemy.Column(sqlalchemy.String, sqlalchemy.ForeignKey('token.token_id'))
    exchange_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('exchange.exchange_id'))
    timestamp = sqlalchemy.Column(sqlalchemy.DateTime, default=datetime.utcnow)
    TVL = sqlalchemy.Column(sqlalchemy.Float)

class YieldValue(Base):
    __tablename__ = 'YieldValue'
    YieldValue_id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    token_id = sqlalchemy.Column(sqlalchemy.String, sqlalchemy.ForeignKey('token.token_id'))
    exchange_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('exchange.exchange_id'))
    timestamp = sqlalchemy.Column(sqlalchemy.DateTime, default=datetime.utcnow)
    YieldValue = sqlalchemy.Column(sqlalchemy.Float)

class Revenue(Base):
    __tablename__ = 'Revenue'
    Revenue_id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    token_id = sqlalchemy.Column(sqlalchemy.String, sqlalchemy.ForeignKey('token.token_id'))
    exchange_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('exchange.exchange_id'))
    timestamp = sqlalchemy.Column(sqlalchemy.DateTime, default=datetime.utcnow)
    Revenue = sqlalchemy.Column(sqlalchemy.Float)

Base.metadata.create_all(engine)

# List of exchanges
exchanges = ['binance', 'mexc', 'bybit', 'coinbase', 'lbank', 'bitget', 'okex', 'gateio', 'kucoin', 'huobi']

# List of tokens traded against USDT
tokens = ['BTC', 'ETH', 'XRP', 'LTC', 'BCH', 'ADA', 'DOT', 'LINK', 'XLM', 'UNI']

# Fetch additional token data from CoinMarketCap API
coinmarketcap_api_key = '4afad171-b583-40ab-b20f-c5f66ec211d4'
coinmarketcap_url = f'https://pro-api.coinmarketcap.com/v1/cryptocurrency/info'

headers = {
    'Accepts': 'application/json',
    'X-CMC_PRO_API_KEY': coinmarketcap_api_key
}

for token in tokens:
    params = {
        'symbol': token
    }

    try:
        response = requests.get(coinmarketcap_url, headers=headers, params=params)
        data = response.json()

        if 'data' in data:
            token_data = data['data'].get(token.upper())

            if token_data:
                # Update or insert token data into the database
                token_entry = session.query(Token).filter_by(token_id=token).first()
                if token_entry:
                    token_entry.name = token_data['name']
                    token_entry.symbol = token_data['symbol']
                else:
                    token_entry = Token(token_id=token, name=token_data['name'], symbol=token_data['symbol'])
                    session.add(token_entry)
                session.commit()

    except requests.exceptions.RequestException as e:
        print(f'Error fetching data for {token} from CoinMarketCap API:', e)


# Fetch data from DeFiLlama
def fetch_defilama_data(token_id):
    """
    Fetch data from DeFiLlama for the given token.

    Args:
        token_id: The ID of the token.

    Returns:
        None.
    """

    url = f'https://api.defilama.com/v1/tokens/{token_id}'
    headers = {
        'Accepts': 'application/json'
    }

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return None
    else:
        raise Exception(f'Error fetching data from DeFiLlama for token {token_id}: {response.status_code}')


for token in tokens:
    try:
        defilama_data = fetch_defilama_data(token)
    except Exception as e:
        print(f'Error fetching data from DeFiLlama for token {token}: {e}')
        continue

    if defilama_data is not None:
        # Update or insert token data into the database
        token_entry = session.query(Token).filter_by(token_id=token).first()
        if token_entry:
            token_entry.market_cap = defilama_data['market_cap']
            token_entry.tv = defilama_data['tv']
            token_entry.yield_value = defilama_data['yield_value']
            token_entry.revenue = defilama_data['revenue']
        else:
            token_entry = Token(token_id=token, market_cap=defilama_data['market_cap'], tv=defilama_data['tv'], yield_value=defilama_data['yield_value'], revenue=defilama_data['revenue'])
            session.add(token_entry)
    session.commit


# Fetch live prices and OHLC data from exchanges
for exchange_name in exchanges:
    try:
        exchange = getattr(ccxt, exchange_name)()
        markets = exchange.load_markets()

        for token in tokens:
            symbol = token + '/USDT'

            # Fetch live price
            try:
                ticker = exchange.fetch_ticker(symbol)
                price_entry = Price(token_id=token, exchange_id=exchange_name, price=ticker['last'])
                session.add(price_entry)
            except ccxt.NetworkError as e:
                print(f'Error fetching live price for {symbol} on {exchange_name}:', e)
                continue

            # Fetch OHLC data
            try:
                ohlcv = exchange.fetch_ohlcv(symbol, timeframe='1h', limit=100)
                for candle in ohlcv:
                    ohlcv_entry = OHLC(token_id=token, exchange_id=exchange_name, timestamp=candle[0], open=candle[1], high=candle[2], low=candle[3], close=candle[4], volume=candle[5])
                    session.add(ohlcv_entry)
            except ccxt.NetworkError as e:
                print(f'Error fetching OHLC data for {symbol} on {exchange_name}:', e)
                continue
        session.commit()
    except:
        print('Error fetching data from exchanges')

print('Done!')
