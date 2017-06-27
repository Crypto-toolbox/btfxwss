# Import Built-Ins
import logging
import json
import time
import hashlib
import hmac
import queue
import os
import shutil
import threading
import datetime

from collections import defaultdict
from itertools import islice
from threading import Thread

# Import Third-Party
from websocket import create_connection, WebSocketTimeoutException
from websocket import WebSocketConnectionClosedException

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

    def __init__(self, key=None, secret=None, addr=None):
        """
        Initializes BtfxWss Instance.
        :param key: Api Key as string
        :param secret: Api secret as string
        :param addr: Websocket API Address
        """
        self.key = key if key else ''
        self.secret = secret if secret else ''
        self.conn = None
        self.addr = addr if addr else 'wss://api.bitfinex.com/ws/2'

        # Set up variables for receiver and main loop threads
        self.running = False
        self._receiver_lock = threading.Lock()
        self._processor_lock = threading.Lock()
        self.q = queue.Queue()
        self.receiver_thread = None
        self.processing_thread = None
        self.controller_thread = None
        self.cmd_q = queue.Queue()

        self.ping_timer = None
        self.timeout = 5
        self._heartbeats = {}
        self._late_heartbeats = {}

        # Set up book-keeping variables & configurations
        self.api_version = None
        self.channels = {}  # Dict for matching channel ids with handlers
        self.channel_labels = {}  # Dict for matching channel ids with names
        self.channel_states = {}  # Dict for matching channel ids with status of each channel (alive/dead)
        self.channel_configs = {}  # Variables, as set by subscribe command
        self.wss_config = {}  # Config as passed by 'config' command

        self.tickers = defaultdict(list)
        self.books = defaultdict(Orderbook)
        self.raw_books = defaultdict(Orderbook)
        self._trades = defaultdict(list)
        self.candles = defaultdict(list)
        self.account = defaultdict(list)

        self._event_handlers = {'error': self._raise_error,
                                'unsubscribed': self._handle_unsubscribed,
                                'subscribed': self._handle_subscribed,
                                'auth': self._handle_subscribed,
                                'unauth': self._handle_unsubscribed,
                                'info': self._handle_info,
                                'pong': self._handle_pong,
                                'conf': self._handle_conf}
        self._data_handlers = {'ticker': self._handle_ticker,
                               'book': self._handle_book,
                               'raw_book': self._handle_raw_book,
                               'candles': self._handle_candles,
                               'trades': self._handle_trades,
                               'auth': self._handle_auth}

        # 1XXXX == Error Code -> raise, 2XXXX == Info Code -> call
        def restart_client():
            self.cmd_q.put('restart')

        self._code_handlers = {'20051': restart_client,
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

    def _controller(self):
        """
        Thread func to allow restarting / stopping of threads, for example
        when receiving a connection reset info message from the wss server.
        :return:
        """
        while self.running:
            try:
                cmd = self.cmd_q.get(timeout=1)
            except TimeoutError:
                continue
            except queue.Empty:
                continue
            if cmd == 'restart':
                self.restart(soft=True)
            elif cmd == 'stop':
                self.stop()

    def _check_heartbeats(self, ts, *args, **kwargs):
        """
        Checks if the heartbeats are on-time. If not, the channel id is escalated
        to self._late_heartbeats and a warning is issued; once a hb is received
        again from this channel, it'll be removed from this dict, and an Info
        message logged.
        :param ts: timestamp, declares when data was received by the client
        :return:
        """
        for chan_id in self._heartbeats:
            if ts - self._heartbeats[chan_id] >= 10:
                if chan_id not in self._late_heartbeats:
                    try:
                        # This is newly late; escalate
                        log.warning("BtfxWss.heartbeats: Channel %s hasn't sent a "
                                    "heartbeat in %s seconds!",
                                    self.channel_labels[chan_id],
                                    ts - self._heartbeats[chan_id])
                        self._late_heartbeats[chan_id] = ts
                    except KeyError:
                        # This channel ID Is not known to us - log and raise
                        log.error("BtfxWss.heartbeats: Channel %s is not "
                                  "registered in the client's registry! "
                                  "Restarting client to avoid errors..", chan_id)
                        raise UnknownChannelError
                else:
                    # We know of this already
                    try:
                        self.ping()
                    except (WebSocketConnectionClosedException, WebSocketTimeoutException):
                        self.cmd_q('restart')
                    continue
            else:
                # its not late
                try:
                    self._late_heartbeats.pop(chan_id)
                except KeyError:
                    # wasn't late before, check next channel
                    continue
                log.info("BtfxWss.heartbeats: Channel %s has sent a "
                         "heartbeat again!", self.channel_labels[chan_id])

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
        self._receiver_lock.acquire()
        log.info("BtfxWss.pause(): Pausing client..")

    def unpause(self):
        """
        Unpauses the client
        :return:
        """
        self._receiver_lock.release()
        log.info("BtfxWss.pause(): Unpausing client..")

    def start(self):
        """
        Start the websocket client threads
        :return:
        """
        self.running = True

        # Start controller thread
        self.controller_thread = Thread(target=self._controller, name='Controller Thread')
        self.controller_thread.start()

        log.info("BtfxWss.start(): Initializing Websocket connection..")
        while self.conn is None:
            try:
                self.conn = create_connection(self.addr, timeout=3)
            except WebSocketTimeoutException:
                self.conn = None
                print("Couldn't create websocket connection - retrying!")

        log.info("BtfxWss.start(): Initializing receiver thread..")
        if not self.receiver_thread:
            self.receiver_thread = Thread(target=self.receive, name='Receiver Thread')
            self.receiver_thread.start()
        else:
            log.info("BtfxWss.start(): Thread not started! "
                     "self.receiver_thread is populated!")

        log.info("BtfxWss.start(): Initializing processing thread..")
        if not self.processing_thread:
            self.processing_thread = Thread(target=self.process, name='Processing Thread')
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
        try:
            self.receiver_thread.join()
            if self.receiver_thread.is_alive():
                time.time(1)
        except AttributeError:
            log.debug("BtfxWss.stop(): Receiver thread was not running!")

        log.info("BtfxWss.stop(): Joining processing thread..")
        try:
            self.processing_thread.join()
            if self.processing_thread.is_alive():
                time.time(1)
        except AttributeError:
            log.debug("BtfxWss.stop(): Processing thread was not running!")

        log.info("BtfxWss.stop(): Closing websocket conection..")
        try:
            self.conn.close()
        except WebSocketConnectionClosedException:
            pass
        except AttributeError:
            # Connection is None
            pass

        self.conn = None
        self.processing_thread = None
        self.receiver_thread = None

        log.info("BtfxWss.stop(): Done!")

    def restart(self, soft=False):
        """
        Restarts client. If soft is True, the client attempts to re-subscribe
        to all channels which it was previously subscribed to.
        :return:
        """
        log.info("BtfxWss.restart(): Restarting client..")
        self.stop()
        self.start()
        # cache channel labels temporarily
        channel_labels = [self.channel_labels[k] for k in self.channel_labels] if soft else None

        # clear previous channel caches
        self.channels = {}
        self.channel_labels = {}
        self.channel_states = {}

        if channel_labels:
            # re-subscribe to channels
            for channel_name, kwargs in channel_labels:
                self._subscribe(channel_name, **kwargs)

    def receive(self):
        """
        Receives incoming websocket messages, and puts them on the Client queue
        for processing.
        :return:
        """
        while self.running:
            if self._receiver_lock.acquire(blocking=False):
                try:
                    raw = self.conn.recv()
                except WebSocketTimeoutException:
                    continue
                except WebSocketConnectionClosedException:
                    # this needs to restart the client, while keeping track
                    # of the currently subscribed channels!
                    self.conn = None
                    self.cmd_q.put('restart')
                except AttributeError:
                    # self.conn is None, idle loop until shutdown of thread
                    continue
                except BlockingIOError:
                    # Resource temporarily unavailable
                    log.info("Receiver Thread: Resource temporarily unavailable!")
                    continue
                msg = time.time(), json.loads(raw)
                log.debug("receiver Thread: Data Received: %s", msg)
                self.q.put(msg)
                self._receiver_lock.release()
            else:
                # The receiver_lock was locked, idling until available
                time.sleep(0.5)

    def process(self):
        """
        Processes the Client queue, and passes the data to the respective
        methods.
        :return:
        """

        while self.running:
            if self._processor_lock.acquire(blocking=False):

                if self.ping_timer:
                    try:
                        self._check_ping()
                    except TimeoutError:
                        log.exception("BtfxWss.ping(): TimedOut! (%ss)" %
                                      self.ping_timer)
                    except (WebSocketConnectionClosedException, WebSocketTimeoutException,
                            ConnectionResetError):
                        log.exception("BtfxWss.ping(): Connection Error!")
                        self.conn = None
                if not self.conn:
                    # The connection was killed - initiate restart
                    self.cmd_q.put('restart')

                skip_processing = False

                try:
                    ts, data = self.q.get(timeout=0.1)
                except queue.Empty:
                    skip_processing = True
                    ts = time.time()

                if not skip_processing:
                    if isinstance(data, list):
                        try:
                            self.handle_data(ts, data)
                        except FaultyPayloadError as e:
                            # Data had unexpected format, log and continue
                            log.exception(e)
                            
                    else:  # Not a list, hence it could be a response
                        try:
                            self.handle_response(ts, data)
                        except UnknownEventError:

                            # We don't know what event this is- restart & log data!
                            log.exception("main() - UnknownEventError: %s",
                                          data)
                            log.info("main() - Shutting Down due to "
                                     "Unknown Error!")
                            self.cmd_q.put('restart')
                        except ConnectionResetError:
                            log.info("processor Thread: Connection Was reset, "
                                     "initiating restart")
                            self.cmd_q.put('restart')

                try:
                    self._check_heartbeats(ts)
                except (WebSocketConnectionClosedException, ConnectionResetError,
                        UnknownChannelError):
                    self.cmd_q.put('restart')

                self._processor_lock.release()
            else:
                time.sleep(0.5)

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
        log.info("handle_response: Handling response %s", resp)
        event = resp['event']
        try:
            self._event_handlers[event](ts, **resp)
        # Handle Non-Critical Errors
        except (InvalidChannelError, InvalidPairError, InvalidBookLengthError,
                InvalidBookPrecisionError) as e:
            log.exception(e)
            print(e)
        except (NotSubscribedError, AlreadySubscribedError) as e:
            log.exception(e)
            print(e)
        except GenericSubscriptionError as e:
            log.exception(e)
            print(e)

        # Handle Critical Errors
        except InvalidEventError as e:
            log.critical("handle_response(): %s; %s", e, resp)
            log.exception(e)
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

        self._heartbeats[chanId] = time.time()

        try:
            channel_key = ('raw_'+channel
                           if kwargs['prec'].startswith('R') and channel == 'book'
                           else channel)
        except KeyError:
            channel_key = channel

        try:
            self.channels[chanId] = self._data_handlers[channel_key]
        except KeyError:
            raise UnknownChannelError()

        # prep kwargs to be used as secondary value in dict key
        try:
            kwargs.pop('event')
        except KeyError:
            pass

        try:
            kwargs.pop('len')
        except KeyError:
            pass

        try:
            kwargs.pop('chanId')
        except KeyError:
            pass

        self.channel_labels[chanId] = (channel_key, kwargs)

    def _handle_unsubscribed(self, *args, chanId=None, **kwargs):
        """
        Handles responses to unsubscribe() commands - removes a channel id from
        the client.
        :param chanId: int, represent channel id as assigned by server
        """
        log.debug("_handle_unsubscribed: %s - %s", chanId, kwargs)
        try:
            self.channels.pop(chanId)
        except KeyError:
            raise NotRegisteredError()

        try:
            self._heartbeats.pop(chanId)
        except KeyError:
            pass

        try:
            self._late_heartbeats.pop(chanId)
        except KeyError:
            pass

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
        try:
            log.info("BtfxWss.ping(): Ping received! (%ss)",
                     ts - self.ping_timer)
        except TypeError:
            log.info("BtfxWss.ping(): Ping received!")
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
        if isinstance(data, list) and len(data) == 1:
            data = data[0]
        self._heartbeats[chan_id] = ts
        if data == 'hb':
            self._handle_hearbeat(ts, chan_id)
            return
        try:
            self.channels[chan_id](ts, chan_id, data)
        except KeyError:
            raise NotRegisteredError("handle_data: %s not registered - "
                                     "Payload: %s" % (chan_id, msg))
        except ValueError as e:
            raise FaultyPayloadError("handle_data(): %s - %s" % (msg, e))

    @staticmethod
    def _handle_hearbeat(*args, **kwargs):
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
        label = self.channel_labels[chan_id][1]['pair']
        entry = (*data, ts,)
        self.tickers[label].append(entry)

    def _handle_book(self, ts, chan_id, data):
        """
        Updates the order book stored in self.books[chan_id]
        :param ts: timestamp, declares when data was received by the client
        :param chan_id: int, channel id
        :param data: dict, tuple or list of data received via wss
        :return:
        """
        log.debug("ts: %s\tchan_id: %s\tdata: %s", ts, chan_id, data)
        label = self.channel_labels[chan_id][1]['pair']

        if all(isinstance(elem, list) for elem in data):
            # snapshot
            for order in data:
                price, count, amount = order
                side = (self.books[label].bids if amount > 0
                        else self.books[label].asks)
                side[str(price)] = (price, amount, count, ts)
        else:
            # update
            price, count, amount = data
            side = (self.books[label].bids if amount > 0
                    else self.books[label].asks)
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
        data = data[0]  # peel off blob remainder
        if isinstance(data[0], list):
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
        Files trades in self._trades[chan_id]
        :param ts: timestamp, declares when data was received by the client
        :param chan_id: int, channel id
        :param data: list of data received via wss
        :return:
        """
        label = self.channel_labels[chan_id][1]['pair']
        if isinstance(data[0], list):
            # snapshot
            for trade in data:
                order_id, mts, amount, price = trade
                self._trades[label][order_id] = (order_id, mts, amount, price)
        else:
            # single data
            _type, trade = data
            self._trades[label][trade[0]] = trade

    def _handle_candles(self, ts, chan_id, data):
        """
        Stores OHLC data received via wss in self.candles[chan_id]
        :param ts: timestamp, declares when data was received by the client
        :param chan_id: int, channel id
        :param data: list of data received via wss
        :return:
        """
        label = self.channel_labels[chan_id][1]['key']
        if all(isinstance(elem, list) for elem in data):
            # snapshot
            for candle in data:
                self.candles[label].append(candle)
        else:
            # update
            self.candles[label].append(data)

    def _handle_auth(self, ts, chan_id, data):
        keys = {'hts': self._handle_auth_trades,
                'te': self._handle_auth_trades, 'tu': self._handle_auth_trades,
                'ps': self._handle_auth_positions,
                'pn': self._handle_auth_positions,
                'pu': self._handle_auth_positions,
                'pc': self._handle_auth_positions,
                'os': self._handle_auth_orders, 'on': self._handle_auth_orders,
                'ou': self._handle_auth_orders, 'oc': self._handle_auth_orders,
                'hos': self._handle_auth_orders, 'ws': self._handle_auth_wallet,
                'wu': self._handle_auth_wallet, 'bs': self._handle_auth_balance,
                'bu': self._handle_auth_balance,
                'mis': self._handle_auth_margin_info,
                'miu': self._handle_auth_margin_info,
                'fis': self._handle_auth_funding_info,
                'fiu': self._handle_auth_funding_info,
                'fos': self._handle_auth_offers, 'fon': self._handle_auth_offers,
                'fou': self._handle_auth_offers, 'foc': self._handle_auth_offers,
                'hfos': self._handle_auth_offers,
                'fcs': self._handle_auth_credits,
                'fcn': self._handle_auth_credits,
                'fcu': self._handle_auth_credits,
                'fcc': self._handle_auth_credits,
                'hfcs': self._handle_auth_credits,
                'fls': self._handle_auth_loans, 'fln': self._handle_auth_loans,
                'flu': self._handle_auth_loans, 'flc': self._handle_auth_loans,
                'hfls': self._handle_auth_loans,
                'hfts': self._handle_auth_funding_trades,
                'fte': self._handle_auth_funding_trades,
                'ftu': self._handle_auth_funding_trades}

        event, *data = data

        try:
            keys[event](event, data)
        except KeyError:
            log.exception('%s; %s', chan_id, data)
            raise UnknownEventError('The Passed event in data[0] is not '
                                    'associated with any data handler!')
        except Exception:
            log.exception("_handle_auth: %s - %s, %s", chan_id, event, data)
            raise

    def _handle_auth_trades(self, event, data):
        self.account['trades'].append((event, data))

    def _handle_auth_positions(self, event, data):
        self.account['positions'].append((event, data))

    def _handle_auth_orders(self, event, data):
        self.account['orders'].append((event, data))

    def _handle_auth_wallet(self, event, data):
        self.account['wallet'].append((event, data))

    def _handle_auth_balance(self, event, data):
        self.account['balance'].append((event, data))

    def _handle_auth_margin_info(self, event, data):
        self.account['margin_info'].append((event, data))

    def _handle_auth_funding_info(self, event, data):
        self.account['funding_info'].append((event, data))

    def _handle_auth_offers(self, event, data):
        self.account['offers'].append((event, data))

    def _handle_auth_credits(self, event, data):
        self.account['credits'].append((event, data))

    def _handle_auth_loans(self, event, data):
        self.account['loans'].append((event, data))

    def _handle_auth_funding_trades(self, event, data):
        self.account['funding_trades'].append((event, data))

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

    def ticker(self, pair, unsubsribe=False, **kwargs):
        """
        Subscribe to the passed pair's ticker channel.
        :param pair: str, Pair to request data for.
        :param kwargs:
        :return:
        """
        if unsubsribe:
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
