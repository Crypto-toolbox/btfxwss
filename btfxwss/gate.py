from threading import Thread, Timer
import websocket
import logging
import time
import json


class PusherConnection(Thread):
    """Customized Connection Class.
    
    Based on https://github.com/ekulyk/PythonPusherClient Connection.
    
    """
    def __init__(self, event_q, url, log_level=logging.INFO, daemon=True, reconnect_interval=10):
        self.event_q = event_q
        self.url = url

        self.socket = None
        self.socket_id = ""

        self.disconnect_called = False
        self.needs_reconnect = False
        self.default_reconnect_interval = reconnect_interval
        self.reconnect_interval = reconnect_interval

        self.pong_timer = None
        self.pong_received = False
        self.pong_timeout = 30

        channels = ['pusher:connection_established', 'pusher:connection_failed',
                    'pusher:pong', 'pusher:ping', 'pusher:error']
        handlers = [self._connect_handler, self._failed_handler,
                    self._pong_handler, self._ping_handler,
                    self._sys_error_handler]
        self.system_handlers = {channels[i]: handlers[i] for i in range(5)}

        self.state = "initialized"

        self.logger = logging.getLogger(self.__module__)  # create a new logger
        if log_level == logging.DEBUG:
            websocket.enableTrace(True)
        self.logger.setLevel(log_level)

        # From Martyn's comment at:
        # https://pusher.tenderapp.com/discussions/problems/36-no-messages-received-after-1-idle-minute-heartbeat
        #   "We send a ping every 5 minutes in an attempt to keep connections
        #   alive..."
        # This is why we set the connection timeout to 5 minutes, since we can
        # expect a pusher heartbeat message every 5 minutes.  Adding 5 sec to
        # account for small timing delays which may cause messages to not be
        # received in exact 5 minute intervals.

        self.connection_timeout = 305
        self.connection_timer = None

        self.ping_interval = 120
        self.ping_timer = None

        super(PusherConnection, self).__init__(daemon=daemon)

    def disconnect(self):
        self.needs_reconnect = False
        self.disconnect_called = True
        if self.socket:
            self.socket.close()
        self.join()

    def reconnect(self, reconnect_interval=None):
        if reconnect_interval is None:
            reconnect_interval = self.default_reconnect_interval

        self.logger.info("Connection: Reconnect in %s" % reconnect_interval)
        self.reconnect_interval = reconnect_interval

        self.needs_reconnect = True
        if self.socket:
            self.socket.close()

    def run(self):
        self._connect()

    def _connect(self):
        self.state = "connecting"

        self.socket = websocket.WebSocketApp(
            self.url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close
        )

        self.socket.run_forever()

        while self.needs_reconnect and not self.disconnect_called:
            self.logger.info("Attempting to connect again in %s seconds."
                             % self.reconnect_interval)
            self.state = "unavailable"
            time.sleep(self.reconnect_interval)

            # We need to set this flag since closing the socket will set it to
            # false
            self.socket.keep_running = True
            self.socket.run_forever()

    def _on_open(self, ws):
        self.logger.info("Connection: Connection opened")
        # Send a ping right away to inform that the connection is alive. If you
        # don't do this, it takes the ping interval to subcribe to channel and
        # events
        self.send_ping()
        self._start_timers()

    def _on_error(self, ws, error):
        self.logger.info("Connection: Error - %s" % error)
        self.state = "failed"
        self.needs_reconnect = True

    def _on_message(self, ws, message):
        self.logger.info("Connection: Message - %s" % message)

        # Stop our timeout timer, since we got some data
        self._stop_timers()

        params = json.loads(message)

        if 'event' in params and 'channel' not in params:
            if 'channel' not in params.keys() and params['event'] in self.system_handlers:
                # This is a System Message, we'll handle this here
                try:
                    self.system_handlers[params['event']](params['data'])
                except Exception as e:
                    self.logger.exception(e)

            else:
                # This is a channel message, let the pusher client handle it.
                self.event_q.put(params)

        # We've handled our data, so restart our connection timeout handler
        self._start_timers()

    def _on_close(self, ws, *args):
        self.logger.info("Connection: Connection closed")
        self.state = "disconnected"
        self._stop_timers()

    def _stop_timers(self):
        if self.ping_timer:
            self.ping_timer.cancel()

        if self.connection_timer:
            self.connection_timer.cancel()

        if self.pong_timer:
            self.pong_timer.cancel()

    def _start_timers(self):
        self._stop_timers()

        self.ping_timer = Timer(self.ping_interval, self.send_ping)
        self.ping_timer.start()

        self.connection_timer = Timer(self.connection_timeout, self._connection_timed_out)
        self.connection_timer.start()

    def send_event(self, event_name, data, channel_name=None):
        event = {'event': event_name, 'data': data}
        if channel_name:
            event['channel'] = channel_name

        self.logger.info("Connection: Sending event - %s" % event)
        try:
            self.socket.send(json.dumps(event))
        except Exception as e:
            self.logger.error("Failed send event: %s" % e)

    def send_ping(self):
        self.logger.info("Connection: ping to pusher")
        try:
            self.socket.send(json.dumps({'event': 'pusher:ping', 'data': ''}))
        except Exception as e:
            self.logger.error("Failed send ping: %s" % e)
        self.pong_timer = Timer(self.pong_timeout, self._check_pong)
        self.pong_timer.start()

    def send_pong(self):
        self.logger.info("Connection: pong to pusher")
        try:
            self.socket.send(json.dumps({'event': 'pusher:pong', 'data': ''}))
        except Exception as e:
            self.logger.error("Failed send pong: %s" % e)

    def _check_pong(self):
        self.pong_timer.cancel()

        if self.pong_received:
            self.pong_received = False
        else:
            self.logger.info("Did not receive pong in time.  Will attempt to reconnect.")
            self.state = "failed"
            self.reconnect()

    def _connect_handler(self, data):
        parsed = json.loads(data)
        self.socket_id = parsed['socket_id']
        self.state = "connected"

    def _failed_handler(self, data):
        self.state = "failed"

    def _ping_handler(self, data):
        self.send_pong()
        # Restart our timers since we received something on the connection
        self._start_timers()

    def _pong_handler(self, data):
        self.logger.info("Connection: pong from pusher")
        self.pong_received = True

    def _sys_error_handler(self, data):
        if 'code' in data:
            error_code = None

            try:
                error_code = int(data['code'])
            except:
                pass

            if error_code is not None:
                self.logger.error("Connection: Received error %s" % error_code)

                if (error_code >= 4000) and (error_code <= 4099):
                    # The connection SHOULD NOT be re-established unchanged
                    self.logger.info("Connection: Error is unrecoverable.  Disconnecting")
                    self.disconnect()
                elif (error_code >= 4100) and (error_code <= 4199):
                    # The connection SHOULD be re-established after backing off
                    self.reconnect()
                elif (error_code >= 4200) and (error_code <= 4299):
                    # The connection SHOULD be re-established immediately
                    self.reconnect(0)
                else:
                    pass
            else:
                self.logger.error("Connection: Unknown error code")
        else:
            self.logger.error("Connection: No error code supplied")

    def _connection_timed_out(self):
        self.logger.info("Did not receive any data in time.  Reconnecting.")
        self.state = "failed"
        self.reconnect()
