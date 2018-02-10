"""Websocket App with a ZMQ publisher connected to it."""
# Import Built-Ins
import logging
import json
import time
import ssl
import hashlib
import hmac

from threading import Thread, Event
from threading import Timer
from collections import OrderedDict

# Import Third-Party
import websocket
import zmq
import requests

# Import Homebrew

# Init Logging Facilities
log = logging.getLogger(__name__)

ZMQ_CONNECTION = None


class WebSocketConnection(Thread):
    """Websocket Connection Process.

    Connects to all available channels of the Bitfinex Websocket API.

    Default API version is 2.0 (Beta).

    Inspired heavily by ekulyk's PythonPusherClient Connection Class
    https://github.com/ekulyk/PythonPusherClient/blob/master/pusherclient/connection.py

    It handles all low-level system messages, such as reconnects, pausing of
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
        self._channels = {}

        # Attribute storing conf calls
        self._config = None

        # Authentication details
        self.key = api_key
        self.secret = api_secret
        self._authenticated = False

        # ZeroMQ Publisher Socket used to relay data
        self.ctx = ctx or zmq.Context().instance()
        self.publisher = None
        self.zmq_addr = zmq_addr or 'ipc:/tmp/btfxwss'

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
        Thread.__init__(self)
        self.daemon = True

    @property
    def channels(self):
        """Return a list of all available channels."""
        return [self._channels[chan_id] for chan_id in self._channels]

    def disconnect(self):
        """Disconnect from the websocket connection and join the Process."""
        self.log.debug("disconnect(): Disconnecting from API..")
        self.reconnect_required.clear()
        self.disconnect_called.set()
        if self.socket:
            self.socket.close()
        self.join(timeout=1)

    def reconnect(self):
        """Issue a reconnection by setting the reconnect_required event."""
        # Reconnect attempt at self.reconnect_interval
        self.log.debug("reconnect(): Initialzion reconnect sequence..")
        self.connected.clear()
        self.reconnect_required.set()
        if self.socket:
            self.socket.close()

    def send_ping(self):
        """Send a ping message to the API and starts pong timers."""
        self.log.debug("send_ping(): Sending ping to API..")
        self.socket.send(json.dumps({'event': 'ping'}))
        self.pong_timer = Timer(self.pong_timeout, self._check_pong)
        self.pong_timer.start()

    def run(self):
        """Run process."""
        self.log.debug("run(): Starting up..")
        self._connect()

    """WEBSOCKET APP MEETHODS."""

    def _on_message(self, ws, message):
        """Handle and pass received data to the appropriate handlers."""
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

        # Process incoming data package
        if isinstance(data, dict) or data[0] == 'hb':
            # This is a system message
            self._system_handler(data, received_at)
        else:
            # This is a list of data
            self._data_handler(data, received_at)

        # We've received data, reset timers
        self._start_timers()

    def _on_close(self, ws, *args):
        """Call methods upon closing of connection."""
        self.log.info("Connection closed")
        self.connected.clear()
        self._stop_timers()

    def _on_open(self, ws):
        """Call methods upon opening of connection."""
        self.log.info("Connection opened")
        self.connected.set()
        self.send_ping()
        self._start_timers()
        if self.reconnect_required.is_set():
            self.log.info("_on_open(): Connection reconnected, re-subscribing..")
        self._subscribe()

    def _on_error(self, ws, error):
        """Handle websocket errors."""
        self.log.info("Connection Error - %s", error)
        self.reconnect_required.set()
        self.connected.clear()

    """INTERNAL METHODS."""

    def _connect(self):
        """Create a websocket connection."""
        self.log.debug("_connect(): Initializing Connection..")
        self.publisher = self.ctx.socket(zmq.PUB)
        self.publisher.bind(self.zmq_addr)
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

    def _subscribe(self):
        """Subscribe to all available channels."""
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
            if channel == 'config':
                payload = {'event': 'conf'}
                payload.update(configs[channel])
                self._send(**payload)
                continue
            elif channel == 'auth':
                self._send(api_key=self.key, secret=self.secret, auth=True)
                continue

            for pair in pairs:
                if isinstance(configs[channel], list):
                    for config in configs[channel]:
                        payload = {'channel': channel, 'event': 'subscribe'}
                        payload.update(config)
                        if 'key' in payload:
                            payload['key'] = 'trade:' + payload['key'] + ':t' + pair
                        else:
                            payload['symbol'] = pair
                        self._send(**payload)
                else:
                    payload = {'channel': channel, 'event': 'subscribe', 'symbol': pair}
                    payload.update(configs[channel])
                    self._send(**payload)

    def _stop_timers(self):
        """Stop ping, pong and connection timers."""
        if self.ping_timer:
            self.ping_timer.cancel()

        if self.connection_timer:
            self.connection_timer.cancel()

        if self.pong_timer:
            self.pong_timer.cancel()
        self.log.debug("_stop_timers(): Timers stopped.")

    def _start_timers(self):
        """Reset and start timers for API data and connection."""
        self.log.debug("_start_timers(): Resetting timers..")
        self._stop_timers()

        # Sends a ping at ping_interval to see if API still responding
        self.ping_timer = Timer(self.ping_interval, self.send_ping)
        self.ping_timer.start()

        # Automatically reconnect if we didnt receive data
        self.connection_timer = Timer(self.connection_timeout,
                                      self._connection_timed_out)
        self.connection_timer.start()

    def _check_pong(self):
        """Check if a Pong message was received."""
        self.pong_timer.cancel()
        if self.pong_received:
            self.log.debug("_check_pong(): Pong received in time.")
            self.pong_received = False
        else:
            self.log.debug("_check_pong(): Pong not received in time. "
                           "Issuing reconnect..")
            self.reconnect()

    def _send(self, api_key=None, secret=None, list_data=None, auth=False, **kwargs):
        """Send the given Payload to the API via the websocket connection.

        :param kwargs: payload paarameters as key=value pairs
        :return:
        """
        if auth:
            nonce = str(int(time.time() * 1000000))
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
        self.log.debug("_send(): Sending payload to API: %s", payload)
        try:
            self.socket.send(payload)
            return payload
        except websocket.WebSocketConnectionClosedException:
            self.log.error("_send(): Did not send out payload %s - client not connected.", kwargs)

    def publish(self, chan_id, data, ts):
        """Send data via ZMQ socket as multipart message.

        :param chan_id: channel id to post this data on
        :param data: payload to relay
        :param ts: timestamp at which this data was received
        :return:
        """
        channel = self._channels[chan_id]
        if chan_id == 0:
            channel += '/' + data[0]

        frames = [json.dumps(x).encode() for x in (channel, data, ts)]
        self.log.info("publish(): Sending frames %s from address %s..",
                      frames, self.zmq_addr)
        self.publisher.send_multipart(frames)

    def _connection_timed_out(self):
        """Issue a reconnection if the connection timed out."""
        self.log.debug("_connection_timed_out(): Fired! Issuing reconnect..")
        self.reconnect()

    """SYSTEM MESSAGE HANDLER."""

    def _system_handler(self, data, ts):
        """Distribute system messages to the appropriate handler.

        System messages include everything that arrives as a dict,
        or a list containing a heartbeat.

        :param data: data to process
        :param ts: timestamp at which this data was received
        :return:
        """
        log.debug("_system_handler(): Received a system message: %s", data)

        # Assert if this is a heartbeat
        if isinstance(data, list):
            self._heartbeat_handler(data)
            return

        event = data.get('event', None)
        code = data.get('code', None)
        if code:
            if code > 19999:
                return self._handle_EVT(data, ts)
            else:
                return self._handle_ERR(data, ts)

        if event == 'pong':
            self.log.debug("[PONG] Received!")
            self.pong_received = True
            return
        elif event == 'info':
            if 'version' in data:
                self.log.info("[INFO] Connected to API Version %s", data['version'])
                return

        elif event in ('subscribed', 'unsubscribed'):
            return self._subscription_handler(event, data, ts)

        elif event in ('auth', 'unauth'):
            if event == 'auth':
                self._authenticated = data.get('userId', None)
            else:
                self._authenticated = False
            return

        elif event == 'conf':
            self.log.info("[CONFIG] Configuration set: %s", data)
            self._config = data
            return

        self.log.error("Unhandled event: %s, data: %s", event, data)

    def _heartbeat_handler(self, data):
        """Handle heartbeat messages."""
        # Restart our timers since we received some data
        self.log.debug("_heartbeat_handler(): Received a heart beat "
                       "from connection!")
        self._start_timers()

    def _subscription_handler(self, event, data, ts):
        if event == 'subscribed':
            chan_name = data.get('channel')
            chan_id = data.get('chanId')
            data.pop('event', None)
            channel_suffix = ' '.join([key + '=' + value
                                       for key, value
                                       in sorted(data.items(), key=lambda x: x[0])])
            full_name = chan_name + ' ' + channel_suffix
            self._channels[chan_id] = full_name
            self.log.info("[SUBSCRIBE] Successfully subscribed to channel %s", full_name)
            return
        elif event == 'unsubscribed':
            try:
                self._channels.pop(data['chanId'])
            except KeyError:
                self.log.warning("[UNSUBSCRIBE] Attempt to remove channel ID %s from self."
                                 "channels failed: No such channel ID was registered!",
                                 data['chanId'])
            self.log.info("[UNSUBSCRIBE] Removed channel registered with channel ID %s",
                          data['chanId'])
            return
        self.log.error("Unhandled subscription event: %s, data: %s", event, data)

    def _handle_ERR(self, data, ts):
        self.log.error("[ERROR] %s: %s", data['code'], data['msg'])

    def _handle_EVT(self, data, ts):
        code = data['code']
        if code == 20051:
            # stop
            self.log.info("[EVT_STOP] - WebSocket Server stopping..")
            self.paused.set()
        elif code == 20060:
            # syncing
            self.log.info("[EVT_RESYNC_START] - Websocket Server syncing...")
            pass
        elif code == 20061:
            # reconnect
            self.paused.clear()
            self.log.info("[EVT_RESYNC_STOP] - WebSocket Server sync complete.")
        else:
            self.log.error("Unhandled ERR message %s", data)

    def _data_handler(self, data, ts):
        """Handle data messages by passing them up to the client."""
        # Pass the data up to the Client
        chan_id, *data = data
        self.publish(chan_id, data, ts)

    """INPUT API METHODS."""

    @staticmethod
    def _prep_auth_payload(channel, data):
        """Generate a payload to send to API."""
        return [0, channel, None, data]

    def new_order(self, **options):
        """Generate a new order.

        See the Bitfinex documentation for option references:
            https://bitfinex.readme.io/v2/reference#ws-input

        :param options: Order options
        :return:
        """
        self._send(list_data=self._prep_auth_payload('on', options))

    def cancel_order(self, **options):
        """Cancel an order.

        See the Bitfinex documentation for option references:
            https://bitfinex.readme.io/v2/reference#ws-input

        :param options: Order options
        :return:
        """
        self._send(list_data=self._prep_auth_payload('oc', options))

    def cancel_multi_orders(self, **options):
        """Cancel multiple orders.

        See the Bitfinex documentation for option references:
            https://bitfinex.readme.io/v2/reference#ws-input

        :param options: Order options
        :return:
        """
        self._send(list_data=self._prep_auth_payload('oc_multi', options))

    def multi_op_orders(self, **options):
        """Execute multiple order operations.

        See the Bitfinex documentation for option references:
            https://bitfinex.readme.io/v2/reference#ws-input

        :param options: Order options
        :return:
        """
        self._send(list_data=self._prep_auth_payload('ox_multi', options))

    def calc(self, **options):
        """Execute a calculation command.

        See the Bitfinex documentation for option references:
            https://bitfinex.readme.io/v2/reference#ws-input

        :param options: Calculation options.
        :return:
        """
        self._send(list_data=self._prep_auth_payload('calc', options))


if __name__ == '__main__':
    logging.basicConfig(filename='zmq.log', filemode='w+', level=logging.DEBUG)

    ctx = zmq.Context().instance()

    c = WebSocketConnection(zmq_addr='tcp://*:66668', ctx=ctx, log_level=logging.DEBUG)

    sock = ctx.socket(zmq.SUB)
    sock.setsockopt(zmq.SUBSCRIBE, b'')
    sock.connect('tcp://localhost:66668')

    c.start()
    while True:
        try:
            frames = sock.recv_multipart()
            print(frames)
        except KeyboardInterrupt:
            c.disconnect()
            sock.close()
            ctx.destroy()
            break
