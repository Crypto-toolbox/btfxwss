# Import Built-Ins
import logging
from collections import defaultdict

# Import Homebrew
from btfxwss.connection import WebSocketConnection
from btfxwss.queue_processor import QueueProcessor


# Init Logging Facilities
log = logging.getLogger(__name__)


class BtfxWss:
    """Websocket Client Interface to Bitfinex WSS API

    It features separate threads for the connection and data handling.
    Data can be accessed using the provided methods.
    """

    def __init__(self, key=None, secret=None, log_level=None, **wss_kwargs):
        """
        Initializes BtfxWss Instance.
        :param key: Api Key as string
        :param secret: Api secret as string
        :param addr: Websocket API Address
        """
        self.key = key if key else ''
        self.secret = secret if secret else ''

        # Set up book-keeping variables & configurations
        self.channel_configs = defaultdict(dict)  # Variables, as set by subscribe command

        self.conn = WebSocketConnection(log_level=log_level,
                                        **wss_kwargs)
        self.queue_processor = QueueProcessor(self.conn.q,
                                              log_level=log_level)

    @property
    def channel_directory(self):
        return self.queue_processor.channel_directory

    def tickers(self, pair):
        key = ('ticker', pair)
        if key in self.queue_processor.tickers:
            return self.queue_processor.tickers[key]
        else:
            raise KeyError(pair)

    def books(self, pair):
        key = ('book', pair)
        if key in self.queue_processor.books:
            return self.queue_processor.books[key]
        else:
            raise KeyError(pair)

    def raw_books(self, pair):
        key = ('raw_book', pair)
        if key in self.queue_processor.raw_books:
            return self.queue_processor.raw_books[key]
        else:
            raise KeyError(pair)

    def trades(self, pair):
        key = ('trades', pair)
        if key in self.queue_processor.trades:
            return self.queue_processor.trades[key]
        else:
            raise KeyError(pair)

    def candles(self, pair, timeframe):
        key = ('candles', pair, timeframe)
        if key in self.queue_processor.candles:
            return self.queue_processor.candles[key]
        else:
            raise KeyError(pair)

    def start(self):
        self.conn.start()
        self.queue_processor.start()

    def stop(self):
        self.conn.disconnect()
        self.queue_processor.join()

    def config(self, decimals_as_strings=True, ts_as_dates=False,
               sequencing=False, **kwargs):
        """Send configuration to websocket server

        :param decimals_as_strings: bool, turn on/off decimals as strings
        :param ts_as_dates: bool, decide to request timestamps as dates instead
        :param sequencing: bool, turn on sequencing
        :param kwargs:
        :return:
        """
        flags = 0
        if decimals_as_strings:
            flags += 8
        if ts_as_dates:
            flags += 32
        if sequencing:
            flags += 65536
        q = {'event': 'conf', 'flags': flags}
        q.update(kwargs)
        self.conn.send(**q)

    def _subscribe(self, channel_name, identifier, **kwargs):
        q = {'event': 'subscribe', 'channel': channel_name}
        q.update(**kwargs)
        log.debug("_subscribe: %s", q)
        self.conn.send(**q)
        self.channel_configs[identifier] = q

    def _unsubscribe(self, channel_name, identifier, **kwargs):

        channel_id = self.channel_directory[(channel_name,)]
        q = {'event': 'unsubscribe', 'chanId': channel_id}
        q.update(kwargs)
        self.conn.send(**q)
        self.channel_configs.pop(identifier)

    def subscribe_to_ticker(self, pair, unsubscribe=False, **kwargs):
        """Subscribe to the passed pair's ticker channel.

        :param pair: str, Pair to request data for.
        :param kwargs:
        :return:
        """
        identifier = ('ticker', pair)
        if unsubscribe:
            self._unsubscribe('ticker', identifier, symbol=pair, **kwargs)
        else:
            self._subscribe('ticker', identifier, symbol=pair, **kwargs)

    def subscribe_to_order_book(self, pair, unsubscribe=False, **kwargs):
        """Subscribe to the passed pair's order book channel.

        :param pair: str, Pair to request data for.
        :param kwargs:
        :return:
        """
        identifier = ('book', pair)
        if unsubscribe:
            self._unsubscribe('book', identifier, symbol=pair, **kwargs)
        else:
            self._subscribe('book', identifier, symbol=pair, **kwargs)

    def subscribe_to_raw_order_book(self, pair, prec=None, unsubscribe=False, **kwargs):
        """Subscribe to the passed pair's raw order book channel.

        :param pair: str, Pair to request data for.
        :param kwargs:
        :return:
        """
        identifier = ('raw_book', pair)
        prec = 'R0' if prec is None else prec
        if unsubscribe:
            self._unsubscribe('book', identifier, pair=pair, prec=prec, **kwargs)
        else:
            self._subscribe('book', identifier, pair=pair, prec=prec, **kwargs)

    def subscribe_to_trades(self, pair, unsubscribe=False, **kwargs):
        """Subscribe to the passed pair's trades channel.

        :param pair: str, Pair to request data for.
        :param kwargs:
        :return:
        """
        identifier = ('trades', pair)
        if unsubscribe:
            self._unsubscribe('trades', identifier, symbol=pair, **kwargs)
        else:
            self._subscribe('trades', identifier, symbol=pair, **kwargs)

    def subscribe_to_candles(self, pair, timeframe=None, unsubcribe=False, **kwargs):
        """Subscribe to the passed pair's OHLC data channel.

        :param pair: str, Pair to request data for.
        :param timeframe: str, {1m, 5m, 15m, 30m, 1h, 3h, 6h, 12h,
                                1D, 7D, 14D, 1M}
        :param kwargs:
        :return:
        """
        identifier = ('candled', pair, timeframe)
        valid_tfs = ['1m', '5m', '15m', '30m', '1h', '3h', '6h', '12h', '1D',
                     '7D', '14D', '1M']
        if timeframe:
            if timeframe not in valid_tfs:
                raise ValueError("timeframe must be any of %s" % valid_tfs)
        else:
            timeframe = '1m'
        pair = 't' + pair if not pair.startswith('t') else pair
        key = 'trade:' + timeframe + ':' + pair
        if unsubcribe:
            self._unsubscribe('candles', identifier, key=key, **kwargs)
        else:
            self._subscribe('candles', identifier, key=key, **kwargs)
