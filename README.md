# bitfinex_wss
Client for Bitfinex Websocket API written in Python

#
Sample Code:
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
    wss.ping()
    t = time.time()
    while time.time() - t < 10:
        time.sleep(1)
    for id in wss.tickers:
        print(wss.tickers[id])
    wss.stop()
```
