# Import Built-Ins
import logging
import time

# Import Homebrew
from btfxwss.connection import WebSocketConnection
from btfxwss.queue_processor import QueueProcessor


# Init Logging Facilities
log = logging.getLogger(__name__)


def is_connected(func):
    def wrapped(self, *args, **kwargs):
        if self.conn and self.conn.connected.is_set():
            return func(self, *args, **kwargs)
        else:
            log.error("Cannot call %s() on unestablished connection!",
                      func.__name__)
            return None
    return wrapped


class BtfxWss:
    """Websocket Client Interface to Bitfinex WSS API

    It features separate threads for the connection and data handling.
    Data can be accessed using the provided methods.
    """

    def __init__(self, key=None, secret=None, log_level=None, **wss_kwargs):
        """
        Initializes BtfxWss Instance.
        :param key: Api Key as string
        :param secret: Api secret as string
        :param addr: Websocket API Address
        """
        self.key = key if key else ''
        self.secret = secret if secret else ''

        self.conn = WebSocketConnection(log_level=log_level,
                                        **wss_kwargs)
        self.queue_processor = QueueProcessor(self.conn.q,
                                              log_level=log_level)

    ##############
    # Properties #
    ##############
    @property
    def channel_configs(self):
        return self.conn.channel_configs

    @property
    def orders(self):
        """Return queue containing open orders associated with the user account.
        
        :return: Queue()
        """
        return self.queue_processor.account['Orders']

    @property
    def orders_new(self):
        """Return queue containing new orders associated with the user account.
        
        :return: Queue()
        """
        return self.queue_processor.account['Order New']

    @property
    def orders_update(self):
        """Return queue containing order updates associated with the user account.
        
        :return: Queue()
        """
        return self.queue_processor.account['Order Update']

    @property
    def orders_cancel(self):
        """Return queue containing order cancellations associated with the user account.
        
        :return: Queue()
        """
        return self.queue_processor.account['Order Cancel']

    @property
    def positions(self):
        """Return queue containing open positions associated with the user
        
        account.
        :return: Queue()
        """
        return self.queue_processor.account['Positions']
    
    @property
    def positions_new(self):
        """Return queue containing new positions associated with the user account.
        
        :return: Queue()
        """
        return self.queue_processor.account['Position New']

    @property
    def positions_update(self):
        """Return queue containing position updates associated with the user account.
        
        :return: Queue()
        """
        return self.queue_processor.account['Position Update']

    @property
    def positions_cancel(self):
        """Return queue containing position cancellations associated with the user account.
        
        :return: Queue()
        """
        return self.queue_processor.account['Position Cancel']
    
    @property
    def funding_offer_new(self):
        """Return queue containing new funding offers associated with the user account.
        
        :return: Queue()
        """
        return self.queue_processor.account['Funding Offer New']
    
    @property
    def funding_offer_update(self):
        """Return queue containing funding offer updates associated with the user account.
        
        :return: Queue()
        """
        return self.queue_processor.account['Funding Offer Update']
    
    @property
    def funding_offer_cancel(self):
        """Return queue containing canceled funding offers associated with the user account.
        
        :return: Queue()
        """
        return self.queue_processor.account['Funding Offer Cancel']
    
    @property
    def funding_credit_new(self):
        """Return queue containing new funding credit associated with the user account.
        
        :return: Queue()
        """
        return self.queue_processor.account['Funding Credit New']
    
    @property
    def funding_credit_update(self):
        """Return queue containing funding credit updates associated with the user account.
        
        :return: Queue()
        """
        return self.queue_processor.account['Funding Credit Update']
    
    @property
    def funding_credit_cancel(self):
        """Return queue containing canceled funding credit associated with the user account.
        
        :return: Queue()
        """
        return self.queue_processor.account['Funding Credit Cancel']
    
    @property
    def funding_loan_new(self):
        """Return queue containing new funding loan associated with the user account.
        
        :return: Queue()
        """
        return self.queue_processor.account['Funding Loan New']
    
    @property
    def funding_loan_update(self):
        """Return queue containing funding loan updates associated with the user account.
        
        :return: Queue()
        """
        return self.queue_processor.account['Funding Loan Update']
    
    @property
    def funding_loan_cancel(self):
        """Return queue containing canceled funding loan associated with the user account.
        
        :return: Queue()
        """
        return self.queue_processor.account['Funding Loan Cancel']

    @property
    def transactions(self):
        """Return history of trades associated with the user account.
        
        :return: Queue()
        """
        return self.queue_processor.account['Trades']

    @property
    def loans(self):
        """Return current loans associated with the user account.
        
        :return: Queue()
        """
        return self.queue_processor.account['Loans']
    
    @property
    def wallets(self):
        """Return wallet balances associated with the user account.
        
        :return: Queue()
        """
        return self.queue_processor.account['Wallets']

    @property
    def balance_info(self):
        """Return balance information associated with the user account.
        
        :return: Queue()
        """
        return self.queue_processor.account['Balance Info']

    @property
    def margin_info(self):
        """Return margin information associated with the user account.
        
        :return: Queue()
        """
        return self.queue_processor.account['Margin Info']

    @property
    def offers(self):
        """Return current offers associated with the user account.
        
        :return: Queue()
        """
        return self.queue_processor.account['Offers']

    @property
    def funding_info(self):
        """Return funding information associated with the user account.
        
        :return: Queue()
        """
        return self.queue_processor.account['Funding Info']

    @property
    def credits(self):
        """Return current credits associated with the user account.
        
        :return: Queue()
        """
        return self.queue_processor.account['Credits']

    @property
    def channel_directory(self):
        """Return channel directory of currently subscribed channels.
        
        :return: Queue()
        """
        return self.queue_processor.channel_directory

    @property
    def funding_trades(self):
        """Return funding trades associated with the user account.
        
        :return: Queue()
        """
        return self.queue_processor.account['Funding_trades']

    @property
    def notifications(self):
        """Return notifications associated with the user account.
        
        :return: Queue()
        """
        return self.queue_processor.account['Notifications']
    
    # DEPRECATED FUNCTIONS
    @property
    def historical_credits(self):
        """Return history of credits associated with the user account.
        
        :return: Queue()
        """
        return self.queue_processor.account['Historical Credits']
    
    @property
    def historical_offers(self):
        """Return history of offers associated with the user account.
        
        :return: Queue()
        """
        return self.queue_processor.account['Historical Offers']
    
    @property
    def historical_loans(self):
        """Return history of loans associated with the user account.
        
        :return: Queue()
        """
        return self.queue_processor.account['Historical Loans']
    @property
    def historical_orders(self):
        """Return history of orders associated with the user account.
        
        :return: Queue()
        """
        return self.queue_processor.account['Historical Orders']

    ##############################################
    # Client Initialization and Shutdown Methods #
    ##############################################

    def start(self):
        """Start the client.

        :return:
        """
        self.conn.start()
        self.queue_processor.start()

    def stop(self):
        """Stop the client.

        :return:
        """
        self.conn.disconnect()
        self.queue_processor.join()

    def reset(self):
        """Reset the client.

        :return:
        """
        self.conn.reconnect()
        while not self.conn.connected.is_set():
            log.info("reset(): Waiting for connection to be set up..")
            time.sleep(1)

        for key in self.channel_configs:
            self.conn.send(**self.channel_configs[key])

    ##########################
    # Data Retrieval Methods #
    ##########################

    def tickers(self, pair):
        """Return a queue containing all received ticker data.

        :param pair:
        :return: Queue()
        """
        key = ('ticker', pair)
        if key in self.queue_processor.tickers:
            return self.queue_processor.tickers[key]
        else:
            raise KeyError(pair)

    def books(self, pair):
        """Return a queue containing all received book data.

        :param pair:
        :return: Queue()
        """
        key = ('book', pair)
        if key in self.queue_processor.books:
            return self.queue_processor.books[key]
        else:
            raise KeyError(pair)

    def raw_books(self, pair):
        """Return a queue containing all received raw book data.

        :param pair:
        :return: Queue()
        """
        key = ('raw_book', pair)
        if key in self.queue_processor.raw_books:
            return self.queue_processor.raw_books[key]
        else:
            raise KeyError(pair)

    def trades(self, pair):
        """Return a queue containing all received trades data.

        :param pair:
        :return: Queue()
        """
        key = ('trades', pair)
        if key in self.queue_processor.trades:
            return self.queue_processor.trades[key]
        else:
            raise KeyError(pair)

    def candles(self, pair, timeframe=None):
        """Return a queue containing all received candles data.

        :param pair: str, Symbol pair to request data for
        :param timeframe: str
        :return: Queue()
        """
        timeframe = '1m' if not timeframe else timeframe
        key = ('candles', pair, timeframe)
        if key in self.queue_processor.candles:
            return self.queue_processor.candles[key]
        else:
            raise KeyError(pair)

    ##########################################
    # Subscription and Configuration Methods #
    ##########################################

    def _subscribe(self, channel_name, identifier, **kwargs):
        q = {'event': 'subscribe', 'channel': channel_name}
        q.update(**kwargs)
        log.debug("_subscribe: %s", q)
        self.conn.send(**q)
        self.channel_configs[identifier] = q

    def _unsubscribe(self, channel_name, identifier, **kwargs):

        channel_id = self.channel_directory[identifier]
        q = {'event': 'unsubscribe', 'chanId': channel_id}
        q.update(kwargs)
        self.conn.send(**q)
        self.channel_configs.pop(identifier)

    def config(self, decimals_as_strings=True, ts_as_dates=False,
               sequencing=False, **kwargs):
        """Send configuration to websocket server

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
        self.conn.send(**q)

    @is_connected
    def subscribe_to_ticker(self, pair, **kwargs):
        """Subscribe to the passed pair's ticker channel.

        :param pair: str, Symbol pair to request data for
        :param kwargs:
        :return:
        """
        identifier = ('ticker', pair)
        self._subscribe('ticker', identifier, symbol=pair, **kwargs)

    @is_connected
    def unsubscribe_from_ticker(self, pair, **kwargs):
        """Unsubscribe to the passed pair's ticker channel.

        :param pair: str, Symbol pair to request data for
        :param kwargs:
        :return:
        """
        identifier = ('ticker', pair)
        self._unsubscribe('ticker', identifier, symbol=pair, **kwargs)

    @is_connected
    def subscribe_to_order_book(self, pair, **kwargs):
        """Subscribe to the passed pair's order book channel.

        :param pair: str, Symbol pair to request data for
        :param kwargs:
        :return:
        """
        identifier = ('book', pair)
        self._subscribe('book', identifier, symbol=pair, **kwargs)

    @is_connected
    def unsubscribe_from_order_book(self, pair, **kwargs):
        """Unsubscribe to the passed pair's order book channel.

        :param pair: str, Symbol pair to request data for
        :param kwargs:
        :return:
        """
        identifier = ('book', pair)
        self._unsubscribe('book', identifier, symbol=pair, **kwargs)

    @is_connected
    def subscribe_to_raw_order_book(self, pair, prec=None, **kwargs):
        """Subscribe to the passed pair's raw order book channel.

        :param pair: str, Symbol pair to request data for
        :param prec:
        :param kwargs:
        :return:
        """
        identifier = ('raw_book', pair)
        prec = 'R0' if prec is None else prec
        self._subscribe('book', identifier, pair=pair, prec=prec, **kwargs)

    @is_connected
    def unsubscribe_from_raw_order_book(self, pair, prec=None, **kwargs):
        """Unsubscribe to the passed pair's raw order book channel.

        :param pair: str, Symbol pair to request data for
        :param prec:
        :param kwargs:
        :return:
        """
        identifier = ('raw_book', pair)
        prec = 'R0' if prec is None else prec
        self._unsubscribe('book', identifier, pair=pair, prec=prec, **kwargs)

    @is_connected
    def subscribe_to_trades(self, pair, **kwargs):
        """Subscribe to the passed pair's trades channel.

        :param pair: str, Symbol pair to request data for
        :param kwargs:
        :return:
        """
        identifier = ('trades', pair)
        self._subscribe('trades', identifier, symbol=pair, **kwargs)

    @is_connected
    def unsubscribe_from_trades(self, pair, **kwargs):
        """Unsubscribe to the passed pair's trades channel.

        :param pair: str, Symbol pair to request data for
        :param kwargs:
        :return:
        """
        identifier = ('trades', pair)
        self._unsubscribe('trades', identifier, symbol=pair, **kwargs)

    @is_connected
    def subscribe_to_candles(self, pair, timeframe=None, **kwargs):
        """Subscribe to the passed pair's OHLC data channel.

        :param pair: str, Symbol pair to request data for
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
        identifier = ('candles', pair, timeframe)
        pair = 't' + pair if not pair.startswith('t') else pair
        key = 'trade:' + timeframe + ':' + pair
        self._subscribe('candles', identifier, key=key, **kwargs)

    @is_connected
    def unsubscribe_from_candles(self, pair, timeframe=None, **kwargs):
        """Unsubscribe to the passed pair's OHLC data channel.

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
        identifier = ('candles', pair, timeframe)
        pair = 't' + pair if not pair.startswith('t') else pair
        key = 'trade:' + timeframe + ':' + pair

        self._unsubscribe('candles', identifier, key=key, **kwargs)

    @is_connected
    def authenticate(self):
        """Authenticate with the Bitfinex API.

        :return:
        """
        if not self.key and not self.secret:
            raise ValueError("Must supply both key and secret key for API!")
        self.channel_configs['auth'] = {'api_key': self.key, 'secret': self.secret}
        self.conn.send(api_key=self.key, secret=self.secret, auth=True)

    @is_connected
    def new_order(self, **order_settings):
        """Post a new Order via Websocket.

        :param kwargs:
        :return:
        """
        self._send_auth_command('on', order_settings)

    @is_connected
    def cancel_order(self, multi=False, **order_identifiers):
        """Cancel one or multiple orders via Websocket.

        :param multi: bool, whether order_settings contains settings for one, or
                      multiples orders
        :param order_identifiers: Identifiers for the order(s) you with to cancel
        :return:
        """
        if multi:
            self._send_auth_command('oc_multi', order_identifiers)
        else:
            self._send_auth_command('oc', order_identifiers)

    @is_connected
    def order_multi_op(self, *operations):
        """Execute multiple, order-related operations via Websocket.

        :param operations: operations to send to the websocket
        :return:
        """
        self._send_auth_command('ox_multi', operations)

    @is_connected
    def calc(self, *calculations):
        """Request one or several operations via Websocket.

        :param calculations: calculations as strings to send to the websocket
        :return:
        """
        self._send_auth_command('calc', calculations)

    def _send_auth_command(self, channel_name, data):
        payload = [0, channel_name, None, data]
        self.conn.send(list_data=payload)
