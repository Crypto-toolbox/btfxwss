# Import Built-Ins
import logging
import json
import time
import hashlib
import hmac
import queue
import os
import shutil
from threading import Thread, Event
import datetime

from collections import defaultdict
from itertools import islice
from threading import Thread

# Import Homebrew
from btfxwss.connection import WebSocketConnection
from btfxwss.queue_processor import QueueProcessor
# import Server-side Exceptions
from btfxwss.exceptions import InvalidBookLengthError, GenericSubscriptionError
from btfxwss.exceptions import NotSubscribedError,  AlreadySubscribedError
from btfxwss.exceptions import InvalidPairError, InvalidChannelError
from btfxwss.exceptions import InvalidEventError, InvalidBookPrecisionError

# import Client-side Exceptions
from btfxwss.exceptions import UnknownEventError, UnknownWSSError
from btfxwss.exceptions import UnknownWSSInfo, AlreadyRegisteredError
from btfxwss.exceptions import NotRegisteredError, UnknownChannelError
from btfxwss.exceptions import FaultyPayloadError


# Init Logging Facilities
log = logging.getLogger(__name__)


class Orders:
    def __init__(self, reverse=False):
        self._orders = {}
        self._reverse = reverse

    def __call__(self):
        return [self._orders[i] for i in sorted(self._orders.keys(),
                                                  reverse=self._reverse)]

    def __repr__(self):
        return str(self.__call__())

    def __setitem__(self, key, value):
        self._orders[key] = value

    def __getitem__(self, key):
        keys = sorted(self._orders.keys(), reverse=self._reverse)

        if isinstance(key, int):
            # an index was passed
            key, = islice(keys, key, key + 1)
            return self._orders[key]
        elif isinstance(key, str) or isinstance(key, float):
            return self._orders[key]
        elif not isinstance(key, slice):
            raise TypeError()

        return [self._orders[key] for key in
                islice(keys, key.start, key.stop,
                       key.step)]

    def pop(self, key):
        return self._orders.pop(key)


class Orderbook:
    def __init__(self):
        self.bids = Orders(reverse=True)
        self.asks = Orders()


class BtfxWss:
    """
    Client Class to connect to Bitfinex Websocket API. Data is stored in attributes.
    Features error handling and logging, as well as reconnection automation if
    the Server issues a connection reset.
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
        self.channel_configs = {}  # Variables, as set by subscribe command

        self.q = queue.Queue()
        self.conn = WebSocketConnection(self.q, log_level=log_level,
                                        **wss_kwargs)
        self.queue_processor = QueueProcessor(self.q, log_level=log_level)

    @property
    def tickers(self):
        return self.queue_processor.tickers

    @property
    def books(self):
        return self.queue_processor.books

    @property
    def raw_books(self):
        return self.queue_processor.raw_books

    @property
    def trades(self):
        return self.queue_processor.trades

    @property
    def candles(self):
        return self.queue_processor.candles

    @property
    def account(self):
        return self.queue_processor.account

    def start(self):
        self.conn.start()
        self.queue_processor.start()

    def stop(self):
        self.conn.disconnect()
        self.queue_processor.join()

    ##
    # Response Message Handlers
    ##


    ##
    # Commands
    ##

    def ping(self):
        """
        Pings Websocket server to check if it's still alive.
        :return:
        """
        self.ping_timer = time.time()
        self.conn.send(json.dumps({'event': 'ping'}))

    def config(self, decimals_as_strings=True, ts_as_dates=False,
               sequencing=False, **kwargs):
        """
        Send configuration to websocket server
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
        self.conn.send(json.dumps(q))

    def _subscribe(self, channel_name, **kwargs):
        if not self.conn:
            log.error("_subscribe(): Cannot subscribe to channel,"
                      "since the client has not been started!")
            return
        q = {'event': 'subscribe', 'channel': channel_name}
        q.update(**kwargs)
        log.debug("_subscribe: %s", q)
        self.conn.send(json.dumps(q))

    def _unsubscribe(self, channel_name, **kwargs):
        if not self.conn:
            log.error("_unsubscribe(): Cannot unsubscribe from channel,"
                      "since the client has not been started!")
            return
        try:
            chan_id = self.channels.pop(channel_name)
        except KeyError:
            raise NotRegisteredError("_unsubscribe(): %s" % channel_name)
        q = {'event': 'unsubscribe', 'chanId': chan_id}
        q.update(kwargs)
        self.conn.send(json.dumps(q))

    def ticker(self, pair, unsubscribe=False, **kwargs):
        """
        Subscribe to the passed pair's ticker channel.
        :param pair: str, Pair to request data for.
        :param kwargs:
        :return:
        """
        if unsubscribe:
            self._unsubscribe('ticker', symbol=pair, **kwargs)
        else:
            self._subscribe('ticker', symbol=pair, **kwargs)

    def order_book(self, pair, unsubscribe=False, **kwargs):
        """
        Subscribe to the passed pair's order book channel.
        :param pair: str, Pair to request data for.
        :param kwargs:
        :return:
        """
        if unsubscribe:
            self._unsubscribe('book', symbol=pair, **kwargs)
        else:
            self._subscribe('book', symbol=pair, **kwargs)

    def raw_order_book(self, pair, prec=None, unsubscribe=False, **kwargs):
        """
        Subscribe to the passed pair's raw order book channel.
        :param pair: str, Pair to request data for.
        :param kwargs:
        :return:
        """
        prec = 'R0' if prec is None else prec
        if unsubscribe:
            self._unsubscribe('book', pair=pair, prec=prec, **kwargs)
        else:
            self._subscribe('book', pair=pair, prec=prec, **kwargs)

    def trades(self, pair, unsubscribe=False, **kwargs):
        """
        Subscribe to the passed pair's trades channel.
        :param pair: str, Pair to request data for.
        :param kwargs:
        :return:
        """
        if unsubscribe:
            self._unsubscribe('trades', symbol=pair, **kwargs)
        else:
            self._subscribe('trades', symbol=pair, **kwargs)

    def ohlc(self, pair, timeframe=None, unsubcribe=False, **kwargs):
        """
        Subscribe to the passed pair's OHLC data channel.
        :param pair: str, Pair to request data for.
        :param timeframe: str, {1m, 5m, 15m, 30m, 1h, 3h, 6h, 12h,
                                1D, 7D, 14D, 1M}
        :param kwargs:
        :return:
        """
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
            self._unsubscribe('candles', key=key, **kwargs)
        else:
            self._subscribe('candles', key=key, **kwargs)

    ##
    # Private Endpoints
    ##

    def authenticate(self, *filters):
        """
        Authenticate with API; this automatically subscribe to all private
        channels available.
        :return:
        """
        nonce = str(int(time.time() * 1000000000))
        payload = 'AUTH' + nonce
        h = hmac.new(self.secret.encode(), payload.encode(), hashlib.sha384)
        signature = h.hexdigest()

        data = {'event': 'auth', 'apiKey': self.key, 'authPayload': payload,
                'authNonce': nonce, 'authSig': signature, 'filter': filters}
        self.conn.send(json.dumps(data))

    def unauth(self):
        js = {'event': 'unauth', 'chanId': 0}
        self.conn.send(json.dumps(js))

class BtfxWssRaw(BtfxWss):
    """
    Bitfinex Websocket API Client. Inherits from BtfxWss, but stores data in raw
    format on disk instead of in-memory. Rotates files every 24 hours by default.
    """

    def __init__(self, key=None, secret=None, addr=None, output_dir=None,
                 rotate_after=None, rotate_at=None, rotate_to=None):
        """
        Initializes BtfxWssRaw Instance.
        :param key: Api Key as string.
        :param secret: Api secret as string.
        :param addr: Websocket API Address.
        :param output_dir: Directory to create file descriptors in.
        :param rotate_after: time in seconds, after which descriptor ought to be
                             rotated. Default 3600*24s.
        :param rotate_at: optional time, date str in format 'HH:MM' (24h clock),
                          at which descriptors should be rotated. Mutually
                          exclusive with rotate_after.
        :param rotate_to: path as str, target folder to copy data to.
        """
        super(BtfxWssRaw, self).__init__(key=key, secret=secret, addr=addr)

        self.tar_dir = output_dir if output_dir else '/tmp/'

        if not rotate_after and not rotate_at:
            self.rotate_after = rotate_after if rotate_after else 3600 * 24
            self.rotate_at = None
        elif rotate_after and rotate_at:
            raise ValueError("Can only specify either rotate_at or rotate_after! Not both!")
        else:
            self.rotate_after = rotate_after
            self.rotate_at = rotate_at

        self.rotate_to = rotate_to if rotate_to else '/var/tmp/data/'

        # File Desciptor Variables
        self.tickers = None
        self.books = None
        self.raw_books = None
        self._trades = None
        self.candles = None
        self.account = None

        self.rotator_thread = None

    def _init_file_descriptors(self, path=None):
        """
        Assign the file descriptors to their respective attributes.
        :param path: str, defaults to self.tar_dir
        :return:
        """
        path = path if path else self.tar_dir
        self.tickers = open(path + 'btfx_tickers.csv', 'a', encoding='UTF-8')
        self.books = open(path + 'btfx_books.csv', 'a', encoding='UTF-8')
        self.raw_books = open(path + 'btfx_rawbooks.csv', 'a', encoding='UTF-8')
        self._trades = open(path + 'btfx_trades.csv', 'a', encoding='UTF-8')
        self.candles = open(path + 'btfx_candles.csv', 'a', encoding='UTF-8')
        if self.key and self.secret:
            self.account = open(path + 'btfx_account.csv', 'a', encoding='UTF-8')

    def _close_file_descriptors(self):
        """
        Closes all file descriptors
        :return:
        """
        self.tickers.close()
        self.books.close()
        self.raw_books.close()
        self._trades.close()
        self.candles.close()
        if self.account:
            self.account.close()

    def _rotate(self):
        """
        Function for the rotator thread, which calls the rotate_descriptors()
        method after the set interval stated in self.rotate_after
        :return:
        """
        t = time.time()
        rotated = False
        while self.running:

            time_to_rotate = ((time.time() - t >= self.rotate_after)
                              if self.rotate_after
                              else (datetime.datetime.now().strftime('%H:%M') ==
                                    self.rotate_at))

            if time_to_rotate and not rotated:
                self._rotate_descriptors(self.rotate_to)
                rotated = True
                t = time.time()
            else:
                rotated = False
                time.sleep(1)

    def _rotate_descriptors(self, target_dir):
        """
        Acquires the processor lock and cCloses file descriptors, renames the
        files and moves them to target_dir.
        Afterwards, opens new batch of file descriptors and releases the lock.
        :param target_dir: str, path.
        :return:
        """
        with self._processor_lock:

            # close file descriptors
            self._close_file_descriptors()

            # Move old files to a new location
            fnames = ['btfx_tickers.csv', 'btfx_books.csv', 'btfx_rawbooks.csv',
                      'btfx_candles.csv', 'btfx_trades.csv']
            if self.account:
                fnames.append('btfx_account.csv')
            date = time.strftime('%Y-%m-%d_%H:%M:%S')
            for fname in fnames:
                ex_name, dtype = fname.split('_')
                new_name = ex_name + '_' + date + '_' + dtype
                src = self.tar_dir+'/'+fname
                tar = target_dir + '/' + new_name
                shutil.copy(src, tar)
                os.remove(src)

            # re-open file descriptors
            self._init_file_descriptors()

    def start(self):
        """
        Start the websocket client threads
        :return:
        """
        self._init_file_descriptors()

        super(BtfxWssRaw, self).start()

        log.info("BtfxWss.start(): Initializing rotator thread..")
        if not self.rotator_thread:
            self.rotator_thread = Thread(target=self._rotate, name='Rotator Thread')
            self.rotator_thread.start()
        else:
            log.info("BtfxWss.start(): Thread not started! "
                     "self.rotator_thread is populated!")

    def stop(self):
        """
        Stop all threads and modules of the client.
        :return:
        """
        super(BtfxWssRaw, self).stop()

        log.info("BtfxWssRaw.stop(): Joining rotator thread..")
        try:
            self.rotator_thread.join()
            if self.rotator_thread.is_alive():
                time.time(1)
        except AttributeError:
            log.debug("BtfxWss.stop(): Rotator thread was not running!")

        self.rotator_thread = None

        log.info("BtfxWssRaw.stop(): Done!")

    ##
    # Data Message Handlers
    ##

    def _handle_ticker(self, ts, chan_id, data):
        """
        Adds received ticker data to self.tickers dict, filed under its channel
        id.
        :param ts: timestamp, declares when data was received by the client
        :param chan_id: int, channel id
        :param data: tuple or list of data received via wss
        :return:
        """
        js = json.dumps((ts, self.channel_labels[chan_id], data))
        self.tickers.write(js + '\n')

    def _handle_book(self, ts, chan_id, data):
        """
        Updates the order book stored in self.books[chan_id]
        :param ts: timestamp, declares when data was received by the client
        :param chan_id: int, channel id
        :param data: dict, tuple or list of data received via wss
        :return:
        """
        js = json.dumps((ts, self.channel_labels[chan_id], data))
        self.books.write(js + '\n')

    def _handle_raw_book(self, ts, chan_id, data):
        """
        Updates the raw order books stored in self.raw_books[chan_id]
        :param ts: timestamp, declares when data was received by the client
        :param chan_id: int, channel id
        :param data: dict, tuple or list of data received via wss
        :return:
        """

        js = json.dumps((ts, self.channel_labels[chan_id], data))
        self.raw_books.write(js + '\n')

    def _handle_trades(self, ts, chan_id, data):
        """
        Files trades in self._trades[chan_id]
        :param ts: timestamp, declares when data was received by the client
        :param chan_id: int, channel id
        :param data: list of data received via wss
        :return:
        """
        js = json.dumps((ts, self.channel_labels[chan_id], data))
        self._trades.write(js + '\n')

    def _handle_candles(self, ts, chan_id, data):
        """
        Stores OHLC data received via wss in self.candles as json
        :param ts: timestamp, declares when data was received by the client
        :param chan_id: int, channel id
        :param data: list of data received via wss
        :return:
        """
        js = json.dumps((ts, self.channel_labels[chan_id], data))
        self.candles.write(js + '\n')

    def _handle_auth(self, ts, chan_id, data):
        """
        Store Account specific data received by authenticating to the API in
        self.account, as json.
        :param ts: timestamp, declares when data was received by the client
        :param chan_id: int, channel id
        :param data: list of data received via wss
        :return:
        """
        js = json.dumps((ts, self.channel_labels[chan_id], data))
        self.account.write(js + '\n')