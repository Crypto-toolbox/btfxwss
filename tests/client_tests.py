import logging
from unittest import TestCase
import unittest
import time
from queue import Empty
from btfxwss import BtfxWss
from websocket import WebSocketConnectionClosedException

logging.basicConfig(filename='test.log', level=logging.DEBUG)
log = logging.getLogger(__name__)


class BtfxWssTests(TestCase):

    def test_subscribing_to_data_works(self):
        wss = BtfxWss(log_level=logging.DEBUG)
        wss.start()
        time.sleep(1)
        wss.subscribe_to_ticker('BTCUSD')
        wss.subscribe_to_candles('BTCUSD')
        wss.subscribe_to_order_book('BTCUSD')
        wss.subscribe_to_raw_order_book('BTCUSD')
        wss.subscribe_to_trades('BTCUSD')
        time.sleep(10)

        try:
            wss.tickers('BTCUSD').get(block=False)
        except Empty:
            self.fail("No ticker data arrived!")

        try:
            wss.candles('BTCUSD', '1m').get(block=False)
        except Empty:
            self.fail("No candles data arrived!")
        except KeyError:
            self.fail("No candles data arrived, key not found! %s" %
                      list(wss.queue_processor.candles.keys()))

        try:
            wss.books('BTCUSD').get(block=False)
        except Empty:
            self.fail("No book data arrived!")

        try:
            wss.raw_books('BTCUSD').get(block=False)
        except Empty:
            self.fail("No war book data arrived!")

        try:
            wss.trades('BTCUSD').get(block=False)
        except Empty:
            self.fail("No trades data arrived!")

        wss.stop()

    def test_is_connected_decorator_works_as_expected(self):
        wss = BtfxWss(log_level=logging.CRITICAL)
        time.sleep(1)
        wss.start()
        try:
            wss.subscribe_to_candles('BTCUSD')
        except WebSocketConnectionClosedException:
            self.fail("Decorator did not work!")
        wss.stop()


if __name__ == '__main__':
    unittest.main(verbosity=2)
