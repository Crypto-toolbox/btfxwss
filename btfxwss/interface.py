from .gate import PusherConnection
from .channel import Channel
import hashlib
import hmac
import logging
import queue
import json
from threading import Thread, Event
from queue import Queue, Empty

VERSION = "0.2.0"


class Pusher(object):
    host = "ws.pusherapp.com"
    client_id = 'PythonPusherClient'
    protocol = 6

    def __init__(self, key, secure=True, secret=None, user_data=None,
                 log_level=logging.INFO, daemon=True, port=None,
                 reconnect_interval=10):
        # Authentication details
        self.key = key
        self.secret = secret
        self.user_data = user_data or {}

        # Connection and event q
        self.url = self._build_url(key, secure, port)
        self.event_q = queue.Queue()
        self.connection = PusherConnection(self.event_q, self.url,
                                           log_level=log_level, daemon=daemon,
                                           reconnect_interval=reconnect_interval)

        # Channel Threads
        self.tickers = Channel('Tickers', self.event_q)
        self.tickers.start()
        self.books = Channel('Books', self.event_q)
        self.books.start()
        self.raw_books = Channel('Raw Books', self.event_q)
        self.raw_books.start()
        self.candles = Channel('Candles', self.event_q)
        self.candles.start()
        self.trades = Channel('Trades', self.event_q)
        self.trades.start()
        self.system_messages = Channel('System', self.event_q)

        # Channel Callback registry
        self.channels = {'ticker': self.tickers, 'books': self.books,
                         'raw_book': self.raw_books, 'candles': self.candles,
                         'trades': self.trades}

        # Callback feeder
        self.event_feeder = Thread(target=self._sort_data)
        self._running = Event()

    def connect(self):
        """Connect to Pusher"""
        self._running.set()
        # Set Channel Threads to listen
        self.tickers.listen()
        self.books.listen()
        self.raw_books.listen()
        self.candles.listen()
        self.trades.listen()

        # Start connection
        self.connection.start()

        # Send out subscriptions

    def disconnect(self):
        """Disconnect from Pusher"""
        self.connection.disconnect()
        # Pause Channel Threads
        self.tickers.pause()
        self.books.pause()
        self.raw_books.pause()
        self.candles.pause()
        self.trades.pause()
        self._running.pause()

    def start(self):
        """Start Channel Threads and put them in wait mode and 
        connect to pusher.
        """
        self.tickers.start()
        self.books.start()
        self.raw_books.start()
        self.candles.start()
        self.trades.start()

    def stop(self):
        """Stop Channel threads, join them and shutdown client"""
        self.disconnect()
        self.tickers.join()
        self.books.join()
        self.raw_books.join()
        self.candles.join()
        self.trades.join()
        self._running.set()

    def subscribe(self, channel_name):
        """Subscribe to a channel

        :param channel_name: The name of the channel to subscribe to.
        :type channel_name: str

        :rtype : Channel
        """
        data = {'channel': channel_name}

        if channel_name.startswith('presence-'):
            data['auth'] = self._generate_presence_key(
                self.connection.socket_id,
                self.key,
                channel_name,
                self.secret,
                self.user_data
            )
            data['channel_data'] = json.dumps(self.user_data)
        elif channel_name.startswith('private-'):
            data['auth'] = self._generate_private_key(
                self.connection.socket_id,
                self.key,
                channel_name,
                self.secret
            )

        self.connection.send_event('pusher:subscribe', data)

        self.channels[channel_name] = Channel(channel_name, self.connection)

        return self.channels[channel_name]

    def unsubscribe(self, channel_name):
        """Unsubscribe from a channel

        :param channel_name: The name of the channel to unsubscribe from.
        :type channel_name: str
        """
        if channel_name in self.channels:
            self.connection.send_event(
                'pusher:unsubscribe', {
                    'channel': channel_name,
                }
            )
            del self.channels[channel_name]

    def channel(self, channel_name):
        """Get an existing channel object by name

        :param channel_name: The name of the channel you want to retrieve
        :type channel_name: str

        :rtype: Channel or None
        """
        return self.channels.get(channel_name)

    def _sort_data(self):
        while self._running.is_set():
            try:
                payload = self.event_q.get(timeout=0.1)
            except Empty:
                continue

            try:
                event_name = payload['event']
                data = payload['data']
                channel_name = payload['channel']
            except KeyError:
                print("ERROR while unpacking data from event q: %s" % payload)
                continue

            if channel_name in self.channels:
                self.channels[channel_name].put((event_name, data))
            else:
                raise ValueError(channel_name)

    @staticmethod
    def _generate_private_key(socket_id, key, channel_name, secret):
        auth_key = ""

        if socket_id and key and channel_name and secret:
            subject = "%s:%s" % (socket_id, channel_name)
            h = hmac.new(secret, subject, hashlib.sha256)
            auth_key = "%s:%s" % (key, h.hexdigest())

        return auth_key

    @staticmethod
    def _generate_presence_key(socket_id, key, channel_name, secret, user_data):
        auth_key = ""

        if socket_id and key and channel_name and secret and user_data:
            subject = "%s:%s:%s" % (socket_id, channel_name, json.dumps(user_data))
            h = hmac.new(secret, subject, hashlib.sha256)
            auth_key = "%s:%s" % (key, h.hexdigest())

        return auth_key

    @classmethod
    def _build_url(cls, key, secure, port=None):
        path = "/app/%s?client=%s&version=%s&protocol=%s" % (
            key,
            cls.client_id,
            VERSION,
            cls.protocol
        )

        proto = "ws"

        if secure:
            proto = "wss"

        if port is None:
            if secure:
                port = 443
            else:
                port = 80

        return "%s://%s:%s%s" % (
            proto,
            cls.host,
            port,
            path
        )
