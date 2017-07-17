# Import Built-Ins
import logging
from threading import Thread, Event
from queue import Empty, Queue
from collections import defaultdict

# Import Third-Party

# Import Homebrew

# Init Logging Facilities
log = logging.getLogger(__name__)


class QueueProcessor(Thread):
    """Data Processing Thread

    It handles and sorts all API messages relating to
    subscription / subscription cancelling, and sorts all data messages into
    queues, organized by data type (book, ticker, etc) and pairs
    ( BTCUSD, ETHBTC, etc).

    """
    def __init__(self, data_q, log_level=None,
                 *args, **kwargs):
        super(QueueProcessor, self).__init__(*args, **kwargs)
        self.q = data_q

        self._response_handlers = {'unsubscribed': self._handle_unsubscribed,
                                   'subscribed': self._handle_subscribed,
                                   'conf': self._handle_conf}
        self._data_handlers = {'ticker': self._handle_ticker,
                               'book': self._handle_book,
                               'raw_book': self._handle_raw_book,
                               'candles': self._handle_candles,
                               'trades': self._handle_trades}

        # Assigns a channel id to a data handler method.
        self._registry = {}

        # dict to translate channel ids to channel identifiers and vice versa
        self.channel_directory = {}

        # dict to register a method with a channel id
        self.channel_handlers = {}

        # Keeps track of last update to a channel by id.
        self.last_update = {}
        self.tickers = defaultdict(Queue)
        self.books = defaultdict(Queue)
        self.raw_books = defaultdict(Queue)
        self.trades = defaultdict(Queue)
        self.candles = defaultdict(Queue)
        self.account = defaultdict(Queue)

        self._stopped = Event()
        self.log = logging.getLogger(self.__module__)
        self.log.setLevel(level=logging.INFO if not log_level else log_level)

    def join(self, timeout=None):
        self._stopped.set()
        super(QueueProcessor, self).join(timeout=timeout)

    def run(self):
        while not self._stopped.is_set():
            try:
                message = self.q.get(timeout=0.1)
            except Empty:
                continue

            dtype, data, ts = message
            if dtype in ('subscribed', 'unsubscribed', 'conf', 'auth', 'unauth'):
                try:
                    self._response_handlers[dtype](dtype, data, ts)
                except KeyError:
                    self.log.error("Dtype '%s' does not have a response "
                                   "handler!", dtype)
            elif dtype == 'data':
                try:
                    channel_id = data[0]

                    # Get channel type associated with this data to the
                    # associated data type (from 'data' to
                    # 'book', 'ticker' or similar
                    channel_type, *_ = self.channel_directory[channel_id]

                    # Run the associated data handler for this channel type.
                    self._data_handlers[channel_type](channel_type, data, ts)
                    # Update time stamps.
                    self.update_timestamps(channel_id, ts)
                except KeyError:
                    self.log.error("Channel ID does not have a data handler! %s",
                                   message)
            else:
                self.log.error("Unknown dtype on queue! %s", message)
                continue

    def _handle_subscribed(self, dtype, data, ts,):
        """
        Handles responses to subscribe() commands - registers a channel id with
        the client and assigns a data handler to it.
        :param chanId: int, represent channel id as assigned by server
        :param channel: str, represents channel name
        """
        self.log.debug("_handle_subscribed: %s - %s - %s", dtype, data, ts)
        channel_name = data.pop('channel')
        channel_id = data.pop('chanId')
        config = data
        if 'pair' in config:
            symbol = config['pair']
        elif 'symbol' in config:
            symbol = config['symbol']
        elif 'key' in config:
            symbol = config['key'].split(':')[2][1:]  #layout type:interval:tPair
        else:
            symbol = None

        if 'prec' in config and config['prec'].startswith('R'):
            channel_name = 'raw_' + channel_name

        self.channel_handlers[channel_id] = self._data_handlers[channel_name]

        # Create a channel_name, symbol tuple to identify channels of same type
        if 'key' in config:
            identifier = (channel_name, symbol, config['key'].split(':')[1])
        else:
            identifier = (channel_name, symbol)
        self.channel_handlers[channel_id] = identifier
        self.channel_directory[(channel_name, symbol)] = channel_id
        self.channel_directory[channel_id] = (channel_name, symbol)
        self.log.info("Subscription succesful for channel %s", identifier)

    def _handle_unsubscribed(self, dtype, data, ts):
        """
        Handles responses to unsubscribe() commands - removes a channel id from
        the client.
        :param chanId: int, represent channel id as assigned by server
        """
        self.log.debug("_handle_unsubscribed: %s - %s - %s", dtype, data, ts)
        channel_id = data.pop('chanId')

        # Unregister the channel from all internal attributes
        chan_identifier = self.channel_directory.pop(channel_id)
        self.channel_directory.pop(chan_identifier)
        self.channel_handlers.pop(channel_id)
        self.last_update.pop(channel_id)
        self.log.info("Successfully unsubscribed from %s", chan_identifier)

    def _handle_conf(self, dtype, data, ts):
        self.log.debug("_handle_conf: %s - %s - %s", dtype, data, ts)
        self.log.info("Configuration accepted: %s", dtype)
        return

    def update_timestamps(self, chan_id, ts):
        try:
            self.last_update[chan_id] = ts
        except KeyError:
            self.log.warning("Attempted ts update of channel %s, but channel "
                             "not present anymore.",
                             self.channel_directory[chan_id])

    def _handle_ticker(self, dtype, data, ts):
        """
        Adds received ticker data to self.tickers dict, filed under its channel
        id.
        :param ts: timestamp, declares when data was received by the client
        :param chan_id: int, channel id
        :param data: tuple or list of data received via wss
        :return:
        """
        self.log.debug("_handle_ticker: %s - %s - %s", dtype, data, ts)
        channel_id, *data = data
        channel_identifier = self.channel_directory[channel_id]

        entry = (data, ts)
        self.tickers[channel_identifier].put(entry)

    def _handle_book(self, dtype, data, ts):
        """
        Updates the order book stored in self.books[chan_id]
        :param ts: timestamp, declares when data was received by the client
        :param chan_id: int, channel id
        :param data: dict, tuple or list of data received via wss
        :return:
        """
        self.log.debug("_handle_book: %s - %s - %s", dtype, data, ts)
        channel_id, *data = data
        log.debug("ts: %s\tchan_id: %s\tdata: %s", ts, channel_id, data)
        channel_identifier = self.channel_directory[channel_id]
        entry = (data, ts)
        self.books[channel_identifier].put(entry)

    def _handle_raw_book(self, dtype, data, ts):
        """
        Updates the raw order books stored in self.raw_books[chan_id]
        :param ts: timestamp, declares when data was received by the client
        :param chan_id: int, channel id
        :param data: dict, tuple or list of data received via wss
        :return:
        """
        self.log.debug("_handle_raw_book: %s - %s - %s", dtype, data, ts)
        channel_id, *data = data
        channel_identifier = self.channel_directory[channel_id]
        entry = (data, ts)
        self.raw_books[channel_identifier].put(entry)

    def _handle_trades(self, dtype, data, ts):
        """
        Files trades in self._trades[chan_id]
        :param ts: timestamp, declares when data was received by the client
        :param chan_id: int, channel id
        :param data: list of data received via wss
        :return:
        """
        self.log.debug("_handle_trades: %s - %s - %s", dtype, data, ts)
        channel_id, *data = data
        channel_identifier = self.channel_directory[channel_id]
        entry = (data, ts)
        self.trades[channel_identifier].put(entry)

    def _handle_candles(self, dtype, data, ts):
        """
        Stores OHLC data received via wss in self.candles[chan_id]
        :param ts: timestamp, declares when data was received by the client
        :param chan_id: int, channel id
        :param data: list of data received via wss
        :return:
        """
        self.log.debug("_handle_candles: %s - %s - %s", dtype, data, ts)
        channel_id, *data = data
        channel_identifier = self.channel_directory[channel_id]
        entry = (data, ts)
        self.candles[channel_identifier].put(entry)

