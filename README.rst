=======
BTFXWSS
=======

Client for Bitfinex Websocket API written in Python

Currently supports all public endpoints; authenticated channels are a
work in progress.

Offers graceful exception handling of common server errors.

Data is stored within `BtfxWss` as `Queue`s. There are convenience
methods available to retrieve a queue for a given type. Consult
the code for more documentation.

Please note that you must take care of handling data in the queues yourself!
Not doing so will eventually result in `MemoryError`s, since the queues
do not have a maximum length defined.

============
Installation
============

Via pip:
```
pip install btfxwss
```


=====
Usage
=====

Starting a session and subscribing to channels.


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
    while not wss.connection.is_connected.is_set():
        time.sleep(1)
    
    # Subscribe to some channels
    wss.subscribe_to_ticker('BTCUSD')
    wss.subscribe_to_order_book('BTCUSD')
    
    # Send a ping - if this returns silently, everything's fine.
    wss.ping()
    
    # Do something else
    t = time.time()
    while time.time() - t < 10:
        pass


Accessing data stored in BtfxWss:

    ticker_q = wss.tickers('BTCUSD')  # returns a Queue object for the pair.
    while not ticker_q.empty():
        print(ticker_q.get())


Unsubscribing from channels:

    wss.ticker('BTCUSD', unsubscribe=True)
    wss.ticker('BTCEUR', unsubscribe=True)


Shutting down the client:


    wss.stop()

Your help is required
=====================

If you find any bugs, error or have feature requests, please don't hesitate to open an issue.
Be as descriptive as possible, and I'll look into the matter as soon as I can.

Thanks
======

A big thanks to the devs providing the `websocket-client <https://github.com/websocket-client/websocket-client>` library,
as well ekulyk for providing the `PythonPusherClient`, which I used as a reference
for the connection class. And finally, a big thanks to all the people submitting
issues, discussing solutions and simply starring the project - you all help me
stay excited and motivated for this project! Cheers to you.




