# bitfinex_wss
Client for Bitfinex Websocket API written in Python

Currently supports all public endpoints; authenticated channels are a
work in progress.

Offers graceful exception handling of common server errors.

Data is stored within the object's attributes for `BtfxWss`;
`BtfxWssRaw` dumps data to a given folder on the disk. 


# Sample Code:

Starting a session and subscribing to channels.

```
    from btfxwss import BtfxWss
    
    logging.basicConfig(level=logging.DEBUG, filename='test.log')
    log = logging.getLogger(__name__)

    fh = logging.FileHandler('test.log')
    fh.setLevel(logging.DEBUG)
    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.DEBUG)

    log.addHandler(sh)
    log.addHandler(fh)
    
    wss = BtfxWss()
    wss.start()
    time.sleep(1)
    wss.ticker('BTCUSD')
    wss.order_book('BTCUSD')
    wss.ping()
    t = time.time()
    while time.time() - t < 10:
        time.sleep(1)
    for id in wss.tickers:
        print(wss.tickers[id])
    wss.stop()
```
Accessing data stored in `BtfxWss`:
```
    print(wss.tickers['BTCUSD])
    print(wss.books['BTCUSD'].bids())  # prints all current bids for the BTCUSD order book
    print(wss.books['BTCUSD'].asks())  # prints all current asks for the BTCUSD order book
```
