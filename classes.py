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

# Init Logging Facilities
log = logging.getLogger(__name__)


class BtfxWss:

    def __init__(self, key='', secret='', addr='wss://api.bitfinex.com/ws/2'):
        self.key = key
        self.secret = secret
        self.conn = None
        self.addr = addr

        # Set up variables for receiver and main loop threads
        self.running = False
        self._paused = False
        self.q = queue.Queue()
        self.receiver_thread = None
        self.main_thread = None

        # Set up book keeping variables
        self.api_version = None
        self.channels = {}  # Dict for matching channel ids with handlers
        self.channel_configs = {}  # Variables, as set by subscribe command
        self.wss_config = {}  # Config as passed by 'config' command

        self.tickers = {}
        self.books = {}
        self.raw_books = {}
        self.trades = {}
        self.candles = {}

        self._event_handlers = {'error': self._handle_error,
                                'unsubscribed': self._handle_unsubscribed,
                                'subscribed': self._handle_subscribed,
                                'info': self._handle_info}
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

    def pause(self):
        self._paused = True

    def unpause(self):
        self._paused = False

    def start(self):
        self.running = True
        self.conn = create_connection(self.addr, timeout=0.1)

        if not self.receiver_thread:
            self.receiver_thread = Thread(target=self.receiver)
            self.receiver_thread.start()

        if not self.main_thread:
            self.main_thread = Thread(target=self.main)
            self.main_thread.start()

    def stop(self):
        self.running = False
        self.receiver_thread.join()
        self.main_thread.join()
        self.conn.close()
        self.conn = None

    def restart(self):
        self.stop()
        self.start()

    def receiver(self):
        while self.running:
            if self._paused:
                time.sleep(0.5)
                continue
            try:
                raw = self.conn.recv()
            except WebSocketTimeoutException:
                continue
            self.q.put(json.loads(raw))

    def main(self):
        while self.running:
            try:
                msg = self.q.get(timeout=0.1)
            except queue.Empty:
                continue
            if isinstance(msg, list):
                self.handle_data(msg)
            else:  # Not a list, hence it could be a response
                try:
                    self.handle_response(msg)
                except UnknownEventError:
                    # We don't know what this is - Raise an error and log data!
                    log.debug("main() - UnknownEventError: %s" % msg)
                    raise

    ##
    # Response Message Handlers
    ##

    def handle_response(self, resp):
        event = resp['event']
        try:
            self._event_handlers[event](**resp)
        except KeyError:
            # unsupported event!
            raise UnknownEventError("handle_response(): %s" % resp)
        except (InvalidChannelError, InvalidPairError, InvalidBookLengthError,
                InvalidBookPrecisionError) as e:
            print("Your request contained one or more invalid values for the "
                  "request you've made. Check your parameters and"
                  "try again.")
        except InvalidEventError as e:
            log.critical("handle_response(): An invalid Event has been sent to "
                         "the API! Check function parameters and code!")
            log.debug("handle_response(): %s" % resp)
            raise SystemError(e)
        except (NotSubscribedError, AlreadySubscribedError) as e:
            print("It seems you're trying to (un)subscribe from a channel "
                  "you're not currently (un)subscribed to! Try Btfx.channels(),"
                  " for a list of channels you've subscribed to!")
        except GenericSubscriptionError as e:
            log.error("handle_response(): An Generic Subscription Error has "
                      "occurred - contact Bitfinex support for more "
                      "information!")
            log.debug("handle_response(): %s" % resp)
            print("A General Subscription Error has occurred: %s" % e)

    def _handle_subscribed(self, chanId=None, channel=None, **kwargs):
        if chanId in self.channels:
            raise AlreadyRegisteredError("_handle_subscribed: Channel ID %s "
                                         "already registered! Restart websocket "
                                         "to empty cache!" % chanId)

        channel_key = ('raw_'+channel
                       if 'pair' in kwargs and channel == 'book'
                       else channel)
        try:
            self.channels[chanId] = self._data_handlers[channel_key]
        except KeyError:
            raise UnknownChannelError("_handle_subscribed(): "
                                      "Key %s not in self.channels!" %
                                      channel_key)

    def _handle_unsubscribed(self, chanId=None, **kwargs):
        try:
            self.channels.pop(chanId)
        except KeyError:
            raise NotRegisteredError("_handle_unsubscribed(): Channel ID %s "
                                          "was not registered with the client! "
                                          "(self.channels: %s)" %
                                          (chanId, list(self.channels.keys())))

    def _handle_error(self, **kwargs):
        error_code = str(kwargs['code'])
        try:
            error_msg = kwargs['msg']
        except KeyError:
            error_msg = error_code

        log.error("_handle_error(): %s" % error_code)
        log.debug("_handle_error(): %s: Attached message: %s" % (error_code, error_msg))

        try:
            raise self._code_handlers[error_code](error_msg)
        except KeyError:
            log.critical("_handle_error(): %s" % error_code)
            log.debug("_handle_error(): %s: Attached message: %s" %
                      (error_code, error_msg))
            raise UnknownWSSError("%s" % kwargs)

    def _handle_info(self, **kwargs):
        if 'version' in kwargs:
            # set api version number and exit
            self.api_version = kwargs['version']
            print("Initialized API with version %s" % self.api_version)
            return

        info_code = str(kwargs['code'])
        if not info_code.startswith('2'):
            raise ValueError("Info Code must start with 2! %s" % kwargs)
        try:
            info_msg = kwargs['msg']
        except KeyError:
            info_msg = info_code

        log.info("_handle_info(): %s: Attached message: %s" %
                 (info_code, info_msg))

        try:
            self._code_handlers[info_code]()
        except KeyError:
            log.error("_handle_info(): %s" % info_code)
            log.debug("_handle_info(): %s: %s" %
                      (info_code, kwargs))
            raise UnknownWSSInfo("_handle_info(): %s" % kwargs)

    ##
    # Data Message Handlers
    ##

    def handle_data(self, msg):
        chan_id, *data = msg
        try:
            self.channels[chan_id](chan_id, data)
        except KeyError:
            raise NotRegisteredError("handle_data: %s not registered - "
                                     "Payload: %s" % (chan_id, msg))

    def _handle_ticker(self, chan_id, data):
        b, b_size, a, a_size, change_24h, change_24_perc, last, vol, h, l = data

    def _handle_book(self, chan_id, data):
        if isinstance(data[0][0], list):
            # snapshot
            for order in data:
                price, count, amount = order
                if amount > 0:
                    # sort in bids
                    pass
                else:
                    # sort in asks
                    pass
        else:
            # update
            price, count, amount = data
            if count == 0:
                # remove from book
                pass
            else:
                # update in book
                pass

    def _handle_raw_book(self, chan_id, data):
        if isinstance(data[0][0], list):
            # snapshot
            pass
        else:
            # update
            order_id, price, amount = data
            if price == 0:
                # remove from book
                pass
            else:
                # update in book
                pass

    def _handle_trades(self, chan_id, data):

        if isinstance(data[0], list):
            # snapshot
            for trade in data:
                order_id, mts, amount, price = trade
        else:
            # update
            _type, trade = data
            execution_update = True if _type == 'tu' else False
            order_id, mts, amount, price = trade
            pass

    def _handle_candles(self, chan_id, data):
        if isinstance(data[0][0], list):
            # snapshot
            for candle in data:
                mts, opn, clse, high, low, vol = candle
                pass
        else:
            # update
            mts, opn, clse, high, low, vol = data

    ##
    # Commands
    ##

    def ping(self):
        self.conn.send(json.dumps({'event': 'ping'}))

    def config(self, **kwargs):
        q = {'event': 'conf'}
        q.update(kwargs)
        self.conn.send(json.dumps(q))

    def _subscribe(self, channel_name, **kwargs):
        q = {'event': 'subscribe', 'channel': channel_name}
        q.update(**kwargs)
        self.conn.send(json.dumps(q))

    def _unsubscribe(self, channel_name):
        try:
            chan_id = self.channels.pop(channel_name)
        except KeyError:
            raise NotRegisteredError("_unsubscribe(): %s" % channel_name)
        q = {'event': 'unsubscribe', 'chanId': chan_id}
        self.conn.send(json.dumps(q))

    def ticker(self, pair, **kwargs):
        self._subscribe(pair, **kwargs)

    def order_book(self, pair, **kwargs):
        self._subscribe('book', symbol=pair, **kwargs)

    def raw_order_book(self, pair, **kwargs):
        self._subscribe('book', pair=pair, **kwargs)

    def trades(self, pair, **kwargs):
        self._subscribe('trades', symbol=pair, **kwargs)

    def ohlc(self, key, **kwargs):
        self._subscribe('candles', key=key, **kwargs)

    ##
    # Private Endpoints
    ##

    def sign(self, **kwargs):
        nonce = str(int(time.time())* 1000)
        auth_msg = 'AUTH' + nonce
        signature = hmac.new(self.secret.encode(), auth_msg.encode(), hashlib.sha384).hexdigest()
        payload = {'apiKey': self.key, 'event': 'auth', 'authPayload': auth_msg,
                   'authNonce': nonce, 'authSig': signature}
        payload.update(**kwargs)
        self.conn.send(json.dumps(payload))




