# Import Built-Ins
import logging
import json
import time
import ssl
import hashlib
import hmac

from multiprocessing import Queue
from threading import Thread, Event, Timer
from collections import OrderedDict

# Import Third-Party
import websocket

# Init Logging Facilities
log = logging.getLogger(__name__)


class WebSocketConnection(Thread):
    """Websocket Connection Thread

    Inspired heavily by ekulyk's PythonPusherClient Connection Class
    https://github.com/ekulyk/PythonPusherClient/blob/master/pusherclient/connection.py

    It handles all low-level system messages, such a reconnects, pausing of
    activity and continuing of activity.
    """
    def __init__(self, *args, url=None, timeout=None, sslopt=None,
                 http_proxy_host=None, http_proxy_port=None, http_proxy_auth=None, http_no_proxy=None,
                 reconnect_interval=None, log_level=None, **kwargs):
        """Initialize a WebSocketConnection Instance.

        :param data_q: Queue(), connection to the Client Class
        :param args: args for Thread.__init__()
        :param url: websocket address, defaults to v2 websocket.
        :param http_proxy_host: proxy host name.
        :param http_proxy_port: http proxy port. If not set, set to 80.
        :param http_proxy_auth: http proxy auth information.
                                tuple of username and password.
        :param http_no_proxy: host names, which doesn't use proxy. 
        :param timeout: timeout for connection; defaults to 10s
        :param reconnect_interval: interval at which to try reconnecting;
                                   defaults to 10s.
        :param log_level: logging level for the connection Logger. Defaults to
                          logging.INFO.
        :param kwargs: kwargs for Thread.__ini__()
        """
        # Queue used to pass data up to BTFX client
        self.q = Queue()

        # Connection Settings
        self.socket = None
        self.url = url if url else 'wss://api.bitfinex.com/ws/2'
        self.sslopt = sslopt if sslopt else {}

        # Proxy Settings
        self.http_proxy_host = http_proxy_host
        self.http_proxy_port = http_proxy_port
        self.http_proxy_auth = http_proxy_auth
        self.http_no_proxy = http_no_proxy

        # Dict to store all subscribe commands for reconnects
        self.channel_configs = OrderedDict()

        # Connection Handling Attributes
        self.connected = Event()
        self.disconnect_called = Event()
        self.reconnect_required = Event()
        self.reconnect_interval = reconnect_interval if reconnect_interval else 10
        self.paused = Event()

        # Setup Timer attributes
        # Tracks API Connection & Responses
        self.ping_timer = None
        self.ping_interval = 120

        # Tracks Websocket Connection
        self.connection_timer = None
        self.connection_timeout = timeout if timeout else 10

        # Tracks responses from send_ping()
        self.pong_timer = None
        self.pong_received = False
        self.pong_timeout = 30

        self.log = logging.getLogger(self.__module__)
        if log_level == logging.DEBUG:
            websocket.enableTrace(True)
        self.log.setLevel(level=log_level if log_level else logging.INFO)

        # Call init of Thread and pass remaining args and kwargs
        Thread.__init__(self)
        self.daemon = True
        # Use default Bitfinex websocket configuration parameters
        self.bitfinex_config = None

    def disconnect(self):
        """Disconnects from the websocket connection and joins the Thread."""
        self.log.debug("disconnect(): Disconnecting from API..")
        self.reconnect_required.clear()
        self.disconnect_called.set()
        if self.socket:
            self.socket.close()
        self.join(timeout=1)

    def reconnect(self):
        """Issues a reconnection by setting the reconnect_required event."""
        # Reconnect attempt at self.reconnect_interval
        self.log.debug("reconnect(): Initialzion reconnect sequence..")
        self.connected.clear()
        self.reconnect_required.set()
        if self.socket:
            self.socket.close()

    def _connect(self):
        """Creates a websocket connection."""
        self.log.debug("_connect(): Initializing Connection..")
        self.socket = websocket.WebSocketApp(
            self.url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close
        )

        if 'ca_certs' not in self.sslopt.keys():
            ssl_defaults = ssl.get_default_verify_paths()
            self.sslopt['ca_certs'] = ssl_defaults.cafile

        self.log.debug("_connect(): Starting Connection..")
        self.socket.run_forever(sslopt=self.sslopt,
                        http_proxy_host=self.http_proxy_host,
                        http_proxy_port=self.http_proxy_port,
                        http_proxy_auth=self.http_proxy_auth,
                        http_no_proxy=self.http_no_proxy)

        # stop outstanding ping/pong timers
        self._stop_timers()
        while self.reconnect_required.is_set():
            if not self.disconnect_called.is_set():
                self.log.info("Attempting to connect again in %s seconds."
                              % self.reconnect_interval)
                self.state = "unavailable"
                time.sleep(self.reconnect_interval)

                # We need to set this flag since closing the socket will
                # set it to False
                self.socket.keep_running = True
                self.socket.sock = None
                self.socket.run_forever(sslopt=self.sslopt,
                                http_proxy_host=self.http_proxy_host,
                                http_proxy_port=self.http_proxy_port,
                                http_proxy_auth=self.http_proxy_auth,
                                http_no_proxy=self.http_no_proxy)
            else:
                break

    def run(self):
        """Main method of Thread."""
        self.log.debug("run(): Starting up..")
        self._connect()

    def _on_message(self, message):
        """Handles and passes received data to the appropriate handlers."""
        self._stop_timers()

        raw, received_at = message, time.time()
        self.log.debug("_on_message(): Received new message %s at %s",
                       raw, received_at)
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            # Something wrong with this data, log and discard
            return

        # Handle data
        if isinstance(data, dict):
            # This is a system message
            self._system_handler(data, received_at)
        else:
            # This is a list of data
            if data[1] == 'hb':
                self._heartbeat_handler()
            else:
                self._data_handler(data, received_at)

        # We've received data, reset timers
        self._start_timers()

    def _on_close(self, *args):
        self.log.info("Connection closed")
        self.connected.clear()
        self._stop_timers()

    def _on_open(self, *args):
        self.log.info("Connection opened")
        self.connected.set()
        self.send_ping()
        self._start_timers()
        if self.reconnect_required.is_set():
            self.log.info("_on_open(): Connection reconnected, re-subscribing..")
            self._resubscribe(soft=False)

    def _on_error(self, error):
        self.log.info("Connection Error - %s", error)
        self.reconnect_required.set()
        self.connected.clear()

    def _stop_timers(self):
        """Stops ping, pong and connection timers."""
        if self.ping_timer:
            self.ping_timer.cancel()

        if self.connection_timer:
            self.connection_timer.cancel()

        if self.pong_timer:
            self.pong_timer.cancel()
        self.log.debug("_stop_timers(): Timers stopped.")

    def _start_timers(self):
        """Resets and starts timers for API data and connection."""
        self.log.debug("_start_timers(): Resetting timers..")
        self._stop_timers()

        # Sends a ping at ping_interval to see if API still responding
        self.ping_timer = Timer(self.ping_interval, self.send_ping)
        self.ping_timer.start()

        # Automatically reconnect if we didnt receive data
        self.connection_timer = Timer(self.connection_timeout,
                                      self._connection_timed_out)
        self.connection_timer.start()

    def send_ping(self):
        """Sends a ping message to the API and starts pong timers."""
        self.log.debug("send_ping(): Sending ping to API..")
        self.socket.send(json.dumps({'event': 'ping'}))
        self.pong_timer = Timer(self.pong_timeout, self._check_pong)
        self.pong_timer.start()

    def _check_pong(self):
        """Checks if a Pong message was received."""
        self.pong_timer.cancel()
        if self.pong_received:
            self.log.debug("_check_pong(): Pong received in time.")
            self.pong_received = False
        else:
            # reconnect
            self.log.debug("_check_pong(): Pong not received in time."
                           "Issuing reconnect..")
            self.reconnect()

    def send(self, api_key=None, secret=None, list_data=None, auth=False, **kwargs):
        """Sends the given Payload to the API via the websocket connection."""
        if auth:
            nonce = str(int(time.time() * 10000000))
            auth_string = 'AUTH' + nonce
            auth_sig = hmac.new(secret.encode(), auth_string.encode(),
                                hashlib.sha384).hexdigest()

            payload = {'event': 'auth', 'apiKey': api_key, 'authSig': auth_sig,
                       'authPayload': auth_string, 'authNonce': nonce}
            payload = json.dumps(payload)
        elif list_data:
            payload = json.dumps(list_data)
        else:
            payload = json.dumps(kwargs)
        self.log.debug("send(): Sending payload to API: %s", payload)
        try:
            self.socket.send(payload)
        except websocket.WebSocketConnectionClosedException:
            self.log.error("send(): Did not send out payload %s - client not connected. ", kwargs)

    def pass_to_client(self, event, data, *args):
        """Passes data up to the client via a Queue()."""
        self.q.put((event, data, *args))

    def _connection_timed_out(self):
        """Issues a reconnection if the connection timed out.

        :return:
        """
        self.log.debug("_connection_timed_out(): Fired! Issuing reconnect..")
        self.reconnect()

    def _pause(self):
        """Pauses the connection.

        :return:
        """
        self.log.debug("_pause(): Setting paused() Flag!")
        self.paused.set()

    def _unpause(self):
        """Unpauses the connection.

        Send a message up to client that he should re-subscribe to all
        channels.
        """
        self.log.debug("_unpause(): Clearing paused() Flag!")
        self.paused.clear()
        self.log.debug("_unpause(): Re-subscribing softly..")
        self._resubscribe(soft=True)

    def _heartbeat_handler(self):
        """Handles heartbeat messages."""
        # Restart our timers since we received some data
        self.log.debug("_heartbeat_handler(): Received a heart beat "
                       "from connection!")
        self._start_timers()

    def _pong_handler(self):
        """Handle a pong response."""
        # We received a Pong response to our Ping!
        self.log.debug("_pong_handler(): Received a Pong message!")
        self.pong_received = True

    def _system_handler(self, data, ts):
        """Distributes system messages to the appropriate handler.

        System messages include everything that arrives as a dict,
        or a list containing a heartbeat.
        """
        self.log.debug("_system_handler(): Received a system message: %s", data)
        # Unpack the data
        event = data.pop('event')
        if event == 'pong':
            self.log.debug(
                "_system_handler(): Distributing %s to _pong_handler..", data
            )
            self._pong_handler()
        elif event == 'info':
            self.log.debug(
                "_system_handler(): Distributing %s to _info_handler..", data
            )
            self._info_handler(data)
        elif event == 'error':
            self.log.debug(
                "_system_handler(): Distributing %s to _error_handler..", data
            )
            self._error_handler(data)
        elif event in ('subscribed', 'unsubscribed', 'conf', 'auth', 'unauth'):
            self.log.debug(
                "_system_handler(): Distributing %s to _response_handler..", data
            )
            self._response_handler(event, data, ts)
        else:
            self.log.error("Unhandled event: %s, data: %s", event, data)

    def _response_handler(self, event, data, ts):
        """Handles responses to (un)subscribe and conf commands.

        Passes data up to client.
        """
        self.log.debug("_response_handler(): Passing %s to client..", data)
        self.pass_to_client(event, data, ts)

    def _info_handler(self, data):
        """Handles INFO messages from the API and issues relevant actions."""

        def raise_exception():
            """Log info code as error and raise a ValueError."""
            self.log.error("%s: %s", data['code'], info_message[data['code']])
            raise ValueError("%s: %s" % (data['code'], info_message[data['code']]))

        if 'code' not in data and 'version' in data:
            self.log.info('Initialized Client on API Version %s', data['version'])
            return
        codes = {
            20000: raise_exception,
            20051: self.reconnect,
            20060: self._pause,
            20061: self._unpause
        }
        info_message = {
            20000: 'Raise exception',
            20051: 'Stop/Restart websocket server (please try to reconnect)',
            20060: 'Refreshing data from the trading engine; please pause any acivity.',
            20061: 'Done refreshing data from the trading engine. Re-subscription advised.'
        }
        try:
            self.log.info(info_message[data['code']])
            codes[data['code']]()
        except KeyError as e:
            self.log.exception(e)
            self.log.error("Unknown Info code %s!", data['code'])
            raise

    def _error_handler(self, data):
        """Handles Error messages and logs them accordingly."""
        try:
            self.log.error("%s: %s", data['code'], data['msg'])
        except KeyError as e:
            log.exception(e)
            self.log.error("Could not parse Error %r! Reconnecting..", data)

    def _data_handler(self, data, ts):
        """Handles data messages by passing them up to the client."""
        # Pass the data up to the Client
        self.log.debug("_data_handler(): Passing %s to client..", data)
        self.pass_to_client('data', data, ts)

    def _resubscribe(self, soft=False):
        """Resubscribes to all channels found in self.channel_configs.

        :param soft: if True, unsubscribes first.
        """
        # Restore non-default Bitfinex websocket configuration
        if self.bitfinex_config:
            self.send(**self.bitfinex_config)
        q_list = []
        while True:
            try:
                identifier, q = self.channel_configs.popitem(last=soft)
            except KeyError:
                break
            q_list.append((identifier, q.copy()))
            if identifier == 'auth':
                self.send(**q, auth=True)
                continue
            if soft:
                q['event'] = 'unsubscribe'
            self.send(**q)

        # Resubscribe for soft start.
        if soft:
            for identifier, q in reversed(q_list):
                self.channel_configs[identifier] = q
                self.send(**q)
        else:
            for identifier, q in q_list:
                self.channel_configs[identifier] = q


class BitfinexEvents:
    """Container class for Bitfinex events."""

    PONG = 'pong'
    INFO = 'info'
    ERROR = 'error'
    SUBSCRIBED = 'subscribed'
    UNSUBSCRIBED = 'unsubscribed'
    CONFIG = 'conf'
    LOGIN = 'auth'
    LOGOUT = 'unauth'
    HEARTBEAT = 'hb'

    #: These events are result of a request sent by the client.
    RESPONSES = [
        SUBSCRIBED,
        UNSUBSCRIBED,
        CONFIG,
        LOGIN,
        LOGOUT,
    ]

    #: All events known to this container class.
    __all__ = [
        PONG,
        INFO,
        ERROR,
        SUBSCRIBED,
        UNSUBSCRIBED,
        CONFIG,
        LOGOUT,
        LOGIN,
        HEARTBEAT,
    ]