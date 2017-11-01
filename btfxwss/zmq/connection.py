# Import Built-Ins
import logging
import json
import time
import ssl
import hashlib
import hmac
from multiprocessing import Event, Process
from threading import Timer
from collections import OrderedDict

# Import Third-Party
import websocket
import zmq
import requests

# Import Homebrew

# Init Logging Facilities
log = logging.getLogger(__name__)


class WebSocketConnection(Process):
    """Websocket Connection Thread

    Inspired heavily by ekulyk's PythonPusherClient Connection Class
    https://github.com/ekulyk/PythonPusherClient/blob/master/pusherclient/connection.py

    It handles all low-level system messages, such a reconnects, pausing of
    activity and continuing of activity.
    """
    def __init__(self, url=None, zmq_addr=None, timeout=None, reconnect_interval=None,
                 log_level=None, ctx=None, api_key=None, api_secret=None):
        """Initialize a WebSocketConnection Instance.

        :param url: websocket address, defaults to v2 websocket.
        :param zmq_addr: ZeroMQ address for the publisher socket.
        :param timeout: timeout for connection; defaults to 10s
        :param reconnect_interval: interval at which to try reconnecting;
                                   defaults to 10s.
        :param log_level: logging level for the connection Logger. Defaults to
                          logging.INFO.
        :param ctx: zmq.Context() to create zmq socket in.
        :param api_key: API key
        :param api_secret: API secret
        """
        # Channel labels for subscriptions
        self.channels = {}

        # Authentication details
        self.key = api_key
        self.secret = api_secret

        # ZeroMQ Publisher Socket used to relay data
        self.ctx = ctx or zmq.Context().instance()
        self.publisher = self.ctx.socket(zmq.PUB)
        self.publisher.bind(zmq_addr or 'ipc:/tmp/btfxwss')

        # Connection Settings for the WebSocket Connection
        self.socket = None
        self.url = url if url else 'wss://api.bitfinex.com/ws/2'

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

        # Setup the internal logger
        # This is done since we are daemonizing the Thread, and thus won't be seeing any log calls
        # from it otherwise.
        self.log = logging.getLogger(self.__module__)
        if log_level == logging.DEBUG:
            websocket.enableTrace(True)
        self.log.setLevel(level=log_level if log_level else logging.INFO)

        # Call init of Thread and pass remaining args and kwargs
        Process.__init__(self)
        self.daemon = True

    def disconnect(self):
        """Disconnects from the websocket connection and joins the Process.

        :return:
        """
        self.log.debug("disconnect(): Disconnecting from API..")
        self.reconnect_required.clear()
        self.disconnect_called.set()
        if self.socket:
            self.socket.close()
        self.join(timeout=1)

    def reconnect(self):
        """Issues a reconnection by setting the reconnect_required event.

        :return:
        """
        # Reconnect attempt at self.reconnect_interval
        self.log.debug("reconnect(): Initialzion reconnect sequence..")
        self.connected.clear()
        self.reconnect_required.set()
        if self.socket:
            self.socket.close()

    def _connect(self):
        """Creates a websocket connection.

        :return:
        """
        self.log.debug("_connect(): Initializing Connection..")
        self.socket = websocket.WebSocketApp(
            self.url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close
        )

        ssl_defaults = ssl.get_default_verify_paths()
        sslopt_ca_certs = {'ca_certs': ssl_defaults.cafile}

        self.log.debug("_connect(): Starting Connection..")
        self.socket.run_forever(sslopt=sslopt_ca_certs)

        while self.reconnect_required.is_set():
            if not self.disconnect_called.is_set():
                self.log.info("Attempting to connect again in %s seconds."
                              % self.reconnect_interval)
                self.state = "unavailable"
                time.sleep(self.reconnect_interval)

                # We need to set this flag since closing the socket will
                # set it to False
                self.socket.keep_running = True
                self.socket.run_forever(sslopt=sslopt_ca_certs)
        self.socket.close()
        self.publisher.close()
        self.ctx.destroy()

    def run(self):
        """Main method of Thread.

        :return:
        """
        self.log.debug("run(): Starting up..")
        self._connect()

    def _on_message(self, ws, message):
        """Handles and passes received data to the appropriate handlers.

        :return:
        """
        self._stop_timers()

        raw, received_at = message, time.time()
        self.log.debug("_on_message(): Received new message %s at %s",
                       raw, received_at)
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as e:
            # Something wrong with this data, log and raise it
            log.exception(e)
            raise

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

    def _on_close(self, ws, *args):
        self.log.info("Connection closed")
        self.connected.clear()
        self._stop_timers()

    def _on_open(self, ws):
        self.log.info("Connection opened")
        self.connected.set()
        self.send_ping()
        self._start_timers()
        if self.reconnect_required.is_set():
            self.log.info("_on_open(): Connection reconnected, re-subscribing..")
        self._subscribe()

    def _on_error(self, ws, error):
        self.log.info("Connection Error - %s", error)
        self.reconnect_required.set()
        self.connected.clear()

    def _stop_timers(self):
        """Stops ping, pong and connection timers.

        :return:
        """
        if self.ping_timer:
            self.ping_timer.cancel()

        if self.connection_timer:
            self.connection_timer.cancel()

        if self.pong_timer:
            self.pong_timer.cancel()
        self.log.debug("_stop_timers(): Timers stopped.")

    def _start_timers(self):
        """Resets and starts timers for API data and connection.

        :return:
        """
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
        """Sends a ping message to the API and starts pong timers.

        :return:
        """
        self.log.debug("send_ping(): Sending ping to API..")
        self.socket.send(json.dumps({'event': 'ping'}))
        self.pong_timer = Timer(self.pong_timeout, self._check_pong)
        self.pong_timer.start()

    def _check_pong(self):
        """Checks if a Pong message was received.

        :return:
        """
        self.pong_timer.cancel()
        if self.pong_received:
            self.log.debug("_check_pong(): Pong received in time.")
            self.pong_received = False
        else:
            self.log.debug("_check_pong(): Pong not received in time."
                           "Issuing reconnect..")
            self.reconnect()

    def send(self,api_key=None, secret=None, list_data=None, auth=False, **kwargs):
        """Sends the given Payload to the API via the websocket connection.

        :param kwargs: payload paarameters as key=value pairs
        :return:
        """
        if auth:
            nonce = str(int(time.time() * 10000000))
            auth_string = 'AUTH' + nonce
            auth_sig = hmac.new(secret.encode(), auth_string.encode(),
                                hashlib.sha384).hexdigest()

            payload = {'event': 'auth', 'apiKey': api_key, 'authSig': auth_sig,
                       'authPayload': auth_string, 'authNonce': nonce}
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
        """Passes data up to the client via a Queue().

        :param event:
        :param data:
        :param args:
        :return:
        """
        pass

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

        :return:
        """
        self.log.debug("_unpause(): Clearing paused() Flag!")
        self.paused.clear()
        self.log.debug("_unpause(): Re-subscribing softly..")
        self._subscribe()

    def _heartbeat_handler(self):
        """Handles heartbeat messages.

        :return:
        """
        # Restart our timers since we received some data
        self.log.debug("_heartbeat_handler(): Received a heart beat "
                       "from connection!")
        self._start_timers()

    def _pong_handler(self):
        """Handle a pong response.

        :return:
        """
        # We received a Pong response to our Ping!
        self.log.debug("_pong_handler(): Received a Pong message!")
        self.pong_received = True

    def _system_handler(self, data, ts):
        """Distributes system messages to the appropriate handler.

        System messages include everything that arrives as a dict,
        or a list containing a heartbeat.

        :param data:
        :param ts:
        :return:
        """
        log.debug("_system_handler(): Received a system message: %s", data)
        # Unpack the data
        event = data.pop('event')
        if event == 'pong':
            log.debug("_system_handler(): Distributing %s to _pong_handler..",
                      data)
            self._pong_handler()
        elif event == 'info':
            log.debug("_system_handler(): Distributing %s to _info_handler..",
                      data)
            self._info_handler(data)
        elif event == 'error':
            log.debug("_system_handler(): Distributing %s to _error_handler..",
                      data)
            self._error_handler(data)
        elif event in ('subscribed', 'unsubscribed', 'conf', 'auth', 'unauth'):
            log.debug("_system_handler(): Distributing %s to "
                      "_response_handler..", data)
            self._response_handler(event, data, ts)
        else:
            self.log.error("Unhandled event: %s, data: %s", event, data)

    def _response_handler(self, event, data, ts):
        """Handle server responses.

        Takes care of setting up new channel labels to broadcast new subscription under, or
        removes them if the response was a confirmation of unsubscribing.

        :param event:
        :param data:
        :param ts:
        :return:
        """
        self.log.debug("_response_handler(): Processing event %s and related data (%s) ..",
                       event, data)
        if event == 'subscribed':
            config = None
            if 'symbol' in data:
                sym = data['symbol']
            elif 'pair' in data:
                sym = data['pair']
            else:
                _, config, sym = data['key'].split(':')
                sym = sym[1:] if sym.startswith('t') else sym

            if not config:
                if 'prec' in data:
                    if data['prec'] == 'R0':
                        config = 'raw'
                    else:
                        config = 'aggregated'

            self.channels[data['chanId']] = data['channel'] + '/' + sym
            if config:
                self.channels[data['chanId']] += '/' + config
            self.log.info("_response_handler(): Registered new channel ID %s as channel %s",
                          data['chanId'], self.channels[data['chanId']])

        elif event == 'unsubscribed':
            try:
                self.channels.pop(data['chanId'])
            except KeyError:
                self.log.warning("_response_handler(): Attempt to remove channel ID %s from self."
                                 "channels failed: No such channel ID was registered!",
                                 data['chanId'])
            self.log.info("_response_handler(): Removed channel registered with channel ID %s",
                          data['chanId'])

        elif event == 'conf':
            self.log.info("_response_handler(): Configuration set: %s", data)
        elif event == 'auth':
            if data['status'] == 'OK':
                self.log.info("_response_handler(): Authentication activated: %s", data)
            else:
                self.log.error("_response_handler(): Authentication failed! %s", data)
        elif event == 'unauth':
            self.log.info("_response_handler(): Authentication deactivated: %s", data)

    def _info_handler(self, data):
        """Handles INFO messages from the API and issues relevant actions.

        :param data:
        :param ts:
        :return:
        """
        codes = {'20051': self.reconnect, '20060': self._pause,
                 '20061': self._unpause}
        info_message = {'20051': 'Stop/Restart websocket server '
                                 '(please try to reconnect)',
                        '20060': 'Refreshing data from the trading engine; '
                                 'please pause any acivity.',
                        '20061': 'Done refreshing data from the trading engine.'
                                 ' Re-subscription advised.'}
        try:
            self.log.info(info_message[data['code']])
            codes[data['code']]()
        except KeyError:
            self.log.warning("_info_handler(): Unkown message format received: %s", data)
            return

    def _error_handler(self, data):
        """Handles Error messages and logs them accordingly.

        :param data:
        :param ts:
        :return:
        """
        errors = {10000: 'Unknown event',
                  10001: 'Unknown pair',
                  10300: 'Subscription Failed (generic)',
                  10301: 'Already Subscribed',
                  10302: 'Unknown channel',
                  10400: 'Subscription Failed (generic)',
                  10401: 'Not subscribed',
                  }
        try:
            self.log.error(errors[data['code']])
        except KeyError:
            self.log.error("Received unknown error Code in message %s! "
                           "Reconnecting..", data)
            self.reconnect()

    def _data_handler(self, data, ts):
        """Handles data messages by passing them up to the client.

        :param data:
        :param ts:
        :return:
        """
        # Pass the data up to the Client
        log.debug("_data_handler(): Passing %s to client..",
                  data)
        self.pass_to_client('data', data, ts)

    def _subscribe(self):
        """Subscribes to all available channels.

        :return: None
        """
        channels = ['config', 'ticker', 'trades', 'book', 'candles']
        if self.key and self.secret:
            channels.append('auth')
        pairs = requests.get('https://api.bitfinex.com/v1/symbols').json()
        configs = {'ticker': {}, 'trades': {}, 'book': [{'prec': 'P0', 'length': 100},
                                                        {'prec': 'R0', 'length': 100}],
                   'candles': [{'key': '1m'}, {'key': '5m'}, {'key': '15m'}, {'key': '30m'},
                               {'key': '1h'}, {'key': '3h'}, {'key': '6h'}, {'key': '12h'},
                               {'key': '1D'}, {'key': '7D'}, {'key': '14D'}, {'key': '1M'}],
                   'auth': {}, 'config': {'flags': 65544}}
        for channel in channels:
            for pair in pairs:
                if isinstance(configs[channel], list):
                    for config in configs[channel]:
                        payload = {'channel': channel, 'event': 'subscribe'}
                        payload.update(config)
                        if 'key' in payload:
                            payload['key'] = 'trade:' + payload['key'] + ':t' + pair
                        self.send(**payload)
                else:
                    if channel == 'auth':
                        self.send(api_key=self.key, secret=self.secret, auth=True)
                    else:
                        payload = {'channel': channel, 'event': 'subscribe'}
                        payload.update(configs[channel])
                        self.send(**payload)


