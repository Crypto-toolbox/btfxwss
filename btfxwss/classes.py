"""
Task:
Descripion of script here.
"""

# Import Built-Ins
import logging
import json
import time
import hashlib
import hmac
import queue
from collections import defaultdict
from itertools import islice

from threading import Thread

# Import Third-Party
from websocket import create_connection, WebSocketTimeoutException

# Import Homebrew
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
log.setLevel(logging.DEBUG)


class Orders:
    def __init__(self, reverse=False):
        self._orders = {}
        self._reverse = reverse

    def __call__(self):
        return [self._orders[i]() for i in sorted(self._orders.keys(),
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

    def __init__(self, key='', secret='', addr='wss://api.bitfinex.com/ws/2',
                 output_dir='/tmp/'):
        self.key = key
        self.secret = secret
        self.conn = None
        self.addr = addr

        # Set up variables for receiver and main loop threads
        self.running = False
        self._paused = False
        self.q = queue.Queue()
        self.receiver_thread = None
        self.processing_thread = None
        self.tar_dir = output_dir
        self.ping_timer = None
        self.timeout = 5
        self._heartbeats = {}

        # Set up book-keeping variables & configurations
        self.api_version = None
        self.channels = {}  # Dict for matching channel ids with handlers
        self.channel_states = {}  # Dict for status of each channel (alive/dead)
        self.channel_configs = {}  # Variables, as set by subscribe command
        self.wss_config = {}  # Config as passed by 'config' command

        self.tickers = defaultdict(list)
        self.books = defaultdict(Orderbook)
        self.raw_books = {}
        self.trades = defaultdict(list)
        self.candles = defaultdict(list)

        self._event_handlers = {'error': self._raise_error,
                                'unsubscribed': self._handle_unsubscribed,
                                'subscribed': self._handle_subscribed,
                                'info': self._handle_info,
                                'pong': self._handle_pong,
                                'conf': self._handle_conf}
        self._data_handlers = {'ticker': self._handle_ticker,
                               'book': self._handle_book,
                               'raw_book': self._handle_raw_book,
                               'candles': self._handle_candles,
                               'trades': self._handle_trades}

        # 1XXXX == Error Code -> raise, 2XXXX == Info Code -> call
        self._code_handlers = {'20051': self.restart,
                               '20060': self.pause,
                               '20061': self.unpause,
                               '10000': InvalidEventError,
                               '10001': InvalidPairError,
                               '10300': GenericSubscriptionError,
                               '10301': AlreadySubscribedError,
                               '10302': InvalidChannelError,
                               '10400': GenericSubscriptionError,
                               '10401': NotSubscribedError,
                               '10011': InvalidBookPrecisionError,
                               '10012': InvalidBookLengthError}

    def _check_heartbeats(self, ts, *args, **kwargs):
        """
        :param ts: timestamp, declares when data was received by the client
        :param chan_id:
        :return:
        """
        for chan_id in self._heartbeats:
            if ts - self._heartbeats[chan_id] > 2:
                log.warning("BtfxWss.heartbeats: Channel %s hasn't send a "
                            "heartbeat in %s seconds!",
                            chan_id, ts - self._heartbeats[chan_id])

    def _check_ping(self):
        """
        Checks if the ping command timed out and raises TimeoutError if so.
        :return:
        """
        if time.time() - self.ping_timer > self.timeout:
            raise TimeoutError("Ping Command timed out!")

    def pause(self):
        """
        Pauses the client
        :return:
        """
        self._paused = True
        log.info("BtfxWss.pause(): Pausing client..")

    def unpause(self):
        """
        Unpauses the client
        :return:
        """
        self._paused = False
        log.info("BtfxWss.pause(): Unpausing client..")

    def start(self):
        """
        Start the websocket client threads
        :return:
        """
        self.running = True
        log.info("BtfxWss.start(): Initializing Websocket connection..")
        self.conn = create_connection(self.addr, timeout=1)

        log.info("BtfxWss.start(): Initializing receiver thread..")
        if not self.receiver_thread:
            self.receiver_thread = Thread(target=self.receive)
            self.receiver_thread.start()
        else:
            log.info("BtfxWss.start(): Thread not started! "
                     "self.receiver_thread is populated!")

        log.info("BtfxWss.start(): Initializing processing thread..")
        if not self.processing_thread:
            self.processing_thread = Thread(target=self.process)
            self.processing_thread.start()
        else:
            log.info("BtfxWss.start(): Thread not started! "
                     "self.processing_thread is populated!")

    def stop(self):
        """
        Stop all threads and modules of the client.
        :return:
        """
        log.info("BtfxWss.stop(): Stopping client..")
        self.running = False
        log.info("BtfxWss.stop(): Joining receiver thread..")
        self.receiver_thread.join()
        log.info("BtfxWss.stop(): Joining processing thread..")
        self.processing_thread.join()
        log.info("BtfxWss.stop(): Closing websocket conection..")
        self.conn.close()
        self.conn = None
        log.info("BtfxWss.stop(): Done!")

    def restart(self):
        """
        Restarts client.
        :return:
        """
        log.info("BtfxWss.restart(): Restarting client..")
        self.stop()
        self.start()

    def receive(self):
        """
        Receives incoming websocket messages, and puts them on the Client queue
        for processing.
        :return:
        """
        while self.running:
            if self._paused:
                time.sleep(0.5)
                continue
            try:
                raw = self.conn.recv()
            except WebSocketTimeoutException:
                continue
            msg = time.time(), json.loads(raw)
            self.q.put(msg)

    def process(self):
        """
        Processes the Client queue, and passes the data to the respective
        methods.
        :return:
        """

        while self.running:

            if self.ping_timer:
                try:
                    self._check_ping()
                except TimeoutError:
                    log.exception("BtfxWss.ping(): TimedOut! (%ss)" %
                                  self.ping_timer)

            skip_processing = False

            try:
                ts, data = self.q.get(timeout=0.1)
            except queue.Empty:
                skip_processing = True

            if not skip_processing:
                if isinstance(data, list):
                    self.handle_data(ts, data)
                else:  # Not a list, hence it could be a response
                    try:
                        self.handle_response(ts, data)
                    except UnknownEventError:
                        # We don't know what this is- Raise an error & log data!
                        log.critical("main() - UnknownEventError: %s", data)
                        raise
            self._check_heartbeats(ts)

    ##
    # Response Message Handlers
    ##

    def handle_response(self, ts, resp):
        """
        Passes a response message to the corresponding event handler, and also
        takes care of handling errors raised by the _raise_error handler.
        :param ts: timestamp, declares when data was received by the client
        :param resp: dict, containing info or error keys, among others
        :return:
        """
        event = resp['event']
        try:
            self._event_handlers[event](ts, **resp)
        # Handle Non-Critical Errors
        except (InvalidChannelError, InvalidPairError, InvalidBookLengthError,
                InvalidBookPrecisionError) as e:
            print(e)
        except (NotSubscribedError, AlreadySubscribedError) as e:
            print(e)
        except GenericSubscriptionError as e:
            print(e)

        # Handle Critical Errors
        except InvalidEventError as e:
            log.critical("handle_response(): %s; %s", e, resp)
            raise SystemError(e)
        except KeyError:
            # unsupported event!
            raise UnknownEventError("handle_response(): %s" % resp)

    def _handle_subscribed(self, *args,  chanId=None, channel=None, **kwargs):
        """
        Handles responses to subscribe() commands - registers a channel id with
        the client and assigns a data handler to it.
        :param chanId: int, represent channel id as assigned by server
        :param channel: str, represents channel name
        """
        log.debug("_handle_subscribed: %s - %s - %s", chanId, channel, kwargs)
        if chanId in self.channels:
            raise AlreadyRegisteredError()

        channel_key = ('raw_'+channel
                       if 'pair' in kwargs and channel == 'book'
                       else channel)
        try:
            self.channels[chanId] = self._data_handlers[channel_key]
        except KeyError:
            raise UnknownChannelError()

    def _handle_unsubscribed(self, *args, chanId=None, **kwargs):
        """
        Handles responses to unsubscribe() commands - removes a channel id from
        the client.
        :param chanId: int, represent channel id as assigned by server
        """
        log.debug("_handle_subscribed: %s - %s", chanId, kwargs)
        try:
            self.channels.pop(chanId)
        except KeyError:
            raise NotRegisteredError()

    def _raise_error(self, *args, **kwargs):
        """
        Raises the proper exception for passed error code. These must then be
        handled by the layer calling _raise_error()
        """
        log.debug("_raise_error(): %s" % kwargs)
        try:
            error_code = str(kwargs['code'])
        except KeyError as e:
            raise FaultyPayloadError('_raise_error(): %s' % kwargs)

        try:
            raise self._code_handlers[error_code]()
        except KeyError:
            raise UnknownWSSError()

    def _handle_info(self, *args, **kwargs):
        """
        Handles info messages and executed corresponding code
        """
        if 'version' in kwargs:
            # set api version number and exit
            self.api_version = kwargs['version']
            print("Initialized API with version %s" % self.api_version)
            return
        try:
            info_code = str(kwargs['code'])
        except KeyError:
            raise FaultyPayloadError("_handle_info: %s" % kwargs)

        if not info_code.startswith('2'):
            raise ValueError("Info Code must start with 2! %s", kwargs)

        output_msg = "_handle_info(): %s" % kwargs
        log.info(output_msg)

        try:
            self._code_handlers[info_code]()
        except KeyError:
            raise UnknownWSSInfo(output_msg)

    def _handle_pong(self, ts, *args, **kwargs):
        """
        Handles pong messages; resets the self.ping_timer variable and logs
        info message.
        :param ts: timestamp, declares when data was received by the client
        :return:
        """
        log.info("BtfxWss.ping(): Ping received! (%ss)",
                 ts - self.ping_timer)
        self.ping_timer = None

    def _handle_conf(self, ts, *args, **kwargs):
        pass

    ##
    # Data Message Handlers
    ##

    def handle_data(self, ts, msg):
        """
        Passes msg to responding data handler, determined by its channel id,
        which is expected at index 0.
        :param ts: timestamp, declares when data was received by the client
        :param msg: list or dict of websocket data
        :return:
        """
        try:
            chan_id, *data = msg
        except ValueError as e:
            # Too many or too few values
            raise FaultyPayloadError("handle_data(): %s - %s" % (msg, e))
        self._heartbeats[chan_id] = ts
        if msg == 'hb':
            self._handle_hearbeat(ts, chan_id)
            return
        try:
            self.channels[chan_id](ts, chan_id, data)
        except KeyError:
            raise NotRegisteredError("handle_data: %s not registered - "
                                     "Payload: %s" % (chan_id, msg))

    def _handle_hearbeat(self, *args, **kwargs):
        """
        By default, does nothing.
        :param args:
        :param kwargs:
        :return:
        """
        pass

    def _handle_ticker(self, ts, chan_id, data):
        """
        Adds received ticker data to self.tickers dict, filed under its channel
        id.
        :param ts: timestamp, declares when data was received by the client
        :param chan_id: int, channel id
        :param data: tuple or list of data received via wss
        :return:
        """
        entry = (*data, ts,)
        self.tickers[chan_id].append(entry)

    def _handle_book(self, ts, chan_id, data):
        """
        Updates the order book stored in self.books[chan_id]
        :param ts: timestamp, declares when data was received by the client
        :param chan_id: int, channel id
        :param data: dict, tuple or list of data received via wss
        :return:
        """
        if isinstance(data[0][0], list):
            # snapshot
            for order in data:
                price, count, amount = order
                side = (self.books[chan_id].bids if amount > 0
                        else self.books[chan_id].asks)
                side[str(price)] = (price, amount, count, ts)
        else:
            # update
            price, count, amount = data
            side = (self.books[chan_id].bids if amount > 0
                    else self.books[chan_id].asks)
            if count == 0:
                # remove from book
                try:
                    side.pop(str(price))
                except KeyError:
                    # didn't exist, move along
                    pass
            else:
                # update in book
                side[str(price)] = (price, amount, count, ts)

    def _handle_raw_book(self, ts, chan_id, data):
        """
        Updates the raw order books stored in self.raw_books[chan_id]
        :param ts: timestamp, declares when data was received by the client
        :param chan_id: int, channel id
        :param data: dict, tuple or list of data received via wss
        :return:
        """
        if isinstance(data[0][0], list):
            # snapshot
            for order in data:
                order_id, price, amount = order
                side = (self.raw_books[chan_id].bids if amount > 0
                        else self.raw_books[chan_id].asks)
                side[str(order_id)] = (order_id, price, amount, ts)
        else:
            # update in book
            order_id, price, amount = data
            side = (self.raw_books[chan_id].bids if amount > 0
                    else self.raw_books[chan_id].asks)
            if price == 0:
                # remove from book
                try:
                    side.pop(str(order_id))
                except KeyError:
                    # didn't exist, move along
                    pass
            else:
                side[str(order_id)] = (order_id, price, amount, ts)

    def _handle_trades(self, ts, chan_id, data):
        """
        Files trades in self.trades[chan_id]
        :param ts: timestamp, declares when data was received by the client
        :param chan_id: int, channel id
        :param data: list of data received via wss
        :return:
        """
        if isinstance(data[0], list):
            # snapshot
            for trade in data:
                order_id, mts, amount, price = trade
                self.trades[chan_id][order_id] = (order_id, mts, amount, price)
        else:
            # single data
            _type, trade = data
            self.trades[chan_id][trade[0]] = trade

    def _handle_candles(self, ts, chan_id, data):
        """
        Stores OHLC data received via wss in self.candles[chan_id]
        :param ts: timestamp, declares when data was received by the client
        :param chan_id: int, channel id
        :param data: list of data received via wss
        :return:
        """
        if isinstance(data[0][0], list):
            # snapshot
            for candle in data:
                self.candles[chan_id].append(candle)
        else:
            # update
            self.candles[chan_id].append(data)

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

    def _unsubscribe(self, channel_name):
        if not self.conn:
            log.error("_unsubscribe(): Cannot unsubscribe from channel,"
                      "since the client has not been started!")
            return
        try:
            chan_id = self.channels.pop(channel_name)
        except KeyError:
            raise NotRegisteredError("_unsubscribe(): %s" % channel_name)
        q = {'event': 'unsubscribe', 'chanId': chan_id}
        self.conn.send(json.dumps(q))

    def ticker(self, pair, **kwargs):
        """
        Subscribe to the passed pair's ticker channel.
        :param pair: str, Pair to request data for.
        :param kwargs:
        :return:
        """
        self._subscribe('ticker', symbol=pair, **kwargs)

    def order_book(self, pair, **kwargs):
        """
        Subscribe to the passed pair's order book channel.
        :param pair: str, Pair to request data for.
        :param kwargs:
        :return:
        """
        self._subscribe('book', symbol=pair, **kwargs)

    def raw_order_book(self, pair, **kwargs):
        """
        Subscribe to the passed pair's raw order book channel.
        :param pair: str, Pair to request data for.
        :param kwargs:
        :return:
        """
        self._subscribe('book', pair=pair, **kwargs)

    def trades(self, pair, **kwargs):
        """
        Subscribe to the passed pair's trades channel.
        :param pair: str, Pair to request data for.
        :param kwargs:
        :return:
        """
        self._subscribe('trades', symbol=pair, **kwargs)

    def ohlc(self, pair, timeframe=None, **kwargs):
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
            timeframe='1m'
        pair = 't' + pair if not pair.startswith('t') else pair
        key = 'trade:' + timeframe + ':' + pair
        self._subscribe('candles', key=key, **kwargs)

    ##
    # Private Endpoints
    ##

    def sign(self, **kwargs):
        """
        Generates signature to authenticate with websocket API.
        :param kwargs:
        :return:
        """
        nonce = str(int(time.time())* 1000)
        auth_msg = 'AUTH' + nonce
        signature = hmac.new(self.secret.encode(), auth_msg.encode(), hashlib.sha384).hexdigest()
        payload = {'apiKey': self.key, 'event': 'auth', 'authPayload': auth_msg,
                   'authNonce': nonce, 'authSig': signature}
        payload.update(**kwargs)
        self.conn.send(json.dumps(payload))
