# Import Built-Ins
import logging
import json
import time
from queue import Queue
from threading import Thread, Event, Timer

# Import Third-Party
import websocket

# Import Homebrew

# Init Logging Facilities
log = logging.getLogger(__name__)


class WebSocketConnection(Thread):
    """Websocket Connection Thread

    Inspired heavily by ekulyk's PythonPusherClient Connection Class
    https://github.com/ekulyk/PythonPusherClient/blob/master/pusherclient/connection.py

    It handles all low-level system messages, such a reconnects, pausing of
    activity and continuing of activity.
    """
    def __init__(self, *args, url=None, timeout=None,
                 reconnect_interval=None, log_level=None, **kwargs):
        """Initialize a WebSocketConnection Instance.

        :param data_q: Queue(), connection to the Client Class
        :param args: args for Thread.__init__()
        :param url: websocket address, defaults to v2 websocket.
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
        self.conn = None
        self.url = url if url else 'wss://api.bitfinex.com/ws/2'

        # Connection Handling Attributes
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

    def disconnect(self):
        """Disconnects from the websocket connection and joins the Thread.

        :return:
        """
        self.reconnect_required.clear()
        self.disconnect_called.set()
        if self.conn:
            self.conn.close()
        self.join(timeout=1)

    def reconnect(self):
        """Issues a reconnection by setting the reconnect_required event.

        :return:
        """
        # Reconnect attempt at self.reconnect_interval
        self.reconnect_required.set()
        if self.conn:
            self.conn.close()

    def _connect(self):
        """Creates a websocket connection.

        :return:
        """
        self.conn = websocket.WebSocketApp(
            self.url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close
        )

        self.conn.run_forever()

        while self.reconnect_required.is_set():
            if not self.disconnect_called.is_set():
                self.log.info("Attempting to connect again in %s seconds."
                              % self.reconnect_interval)
                self.state = "unavailable"
                time.sleep(self.reconnect_interval)

                # We need to set this flag since closing the socket will
                # set it to False
                self.conn.keep_running = True
                self.conn.run_forever()

    def run(self):
        """Main method of Thread.

        :return:
        """
        self._connect()

    def _on_message(self, ws, message):
        """Handles and passes received data to the appropriate handlers.

        :return:
        """
        self._stop_timers()

        raw, received_at = message, time.time()


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

    def _on_close(self, ws, *args):
        self.log.info("Connection closed")
        self._stop_timers()

    def _on_open(self, ws):
        self.log.info("Connection opened")
        self.send_ping()
        self._start_timers()

    def _on_error(self, ws, error):
        self.log.info("Connection Error - %s", error)
        self.reconnect_required.set()

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

    def _start_timers(self):
        """Resets and starts timers for API data and connection.

        :return:
        """
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
        self.conn.send(json.dumps({'event': 'ping'}))
        self.pong_timer = Timer(self.pong_timeout, self._check_pong)
        self.pong_timer.start()

    def _check_pong(self):
        """Checks if a Pong message was received.

        :return:
        """
        self.pong_timer.cancel()
        if self.pong_received:
            self.pong_received = False
        else:
            # reconnect
            self.reconnect()

    def send(self, **kwargs):
        """Sends the given Payload to the API via the websocket connection.

        :param kwargs: payload paarameters as key=value pairs
        :return:
        """
        payload = json.dumps(kwargs)
        self.conn.send(payload)

    def pass_to_client(self, event, data, *args):
        """Passes data up to the client via a Queue().

        :param event:
        :param data:
        :param args:
        :return:
        """
        self.q.put((event, data, *args))

    def _connection_timed_out(self):
        """Issues a reconnection if the connection timed out.

        :return:
        """
        self.reconnect()

    def _pause(self):
        """Pauses the connection.

        :return:
        """
        self.paused.set()

    def _unpause(self):
        """Unpauses the connection.

        Send a message up to client that he should re-subscribe to all
        channels.

        :return:
        """
        self.paused.clear()

        self.pass_to_client('re-subscribe', None)

    def _heartbeat_handler(self):
        """Handles heartbeat messages.

        :return:
        """
        # Restart our timers since we received some data
        self._start_timers()

    def _pong_handler(self):
        """Handle a pong response.

        :return:
        """
        # We received a Pong response to our Ping!
        self.pong_received = True

    def _system_handler(self, data, ts):
        """Distributes system messages to the appropriate handler.

        System messages include everything that arrives as a dict,
        or a list containing a heartbeat.

        :param data:
        :param ts:
        :return:
        """
        # Unpack the data
        event = data.pop('event')
        if event == 'pong':
            self._pong_handler()
        elif event == 'info':
            self._info_handler(data)
        elif event == 'error':
            self._error_handler(data)
        elif event in ('subscribed', 'unsubscribed', 'conf', 'auth', 'unauth'):
            self._response_handler(event, data, ts)
        else:
            self.log.error("Unhandled event: %s, data: %s", event, data)

    def _response_handler(self, event, data, ts):
        """Handles responses to (un)subscribe and conf commands.

        Passes data up to client.

        :param data:
        :param ts:
        :return:
        """
        self.pass_to_client(event, data, ts)

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
            # Unknonw info code, log it
            return

    def _error_handler(self, data):
        """Handles Error messages and logs them accordingly.

        :param data:
        :param ts:
        :return:
        """
        errors = {'10000': 'Unknown event',
                  '10001': 'Unknown pair',
                  '10300': 'Subscription Failed (generic)',
                  '10301': 'Already Subscribed',
                  '10302': 'Unknown channel',
                  '10400': 'Subscription Failed (generic)',
                  '10401': 'Not subscribed',
                  }
        try:
            self.log.error(errors[data['code']])
        except KeyError:
            # Unknown error code, log it and reconnect.
            self.log.error("Received unknown error Code in message %s! "
                           "Reconnecting..", data)

    def _data_handler(self, data, ts):
        """Handles data messages by passing them up to the client.

        :param data:
        :param ts:
        :return:
        """
        # Pass the data up to the Client
        self.pass_to_client('data', data, ts)





