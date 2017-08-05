from unittest import TestCase
import unittest
import time
from queue import Empty
from btfxwss import BtfxWss


class BtfxWssTests(TestCase):

    def test_subscribing_to_ticker_data_works(self):
        wss = BtfxWss()
        wss.start()
        time.sleep(1)
        wss.subscribe_to_ticker('BTCUSD')
        time.sleep(5)

        try:
            wss.tickers('BTCUSD').get(block=False)
        except Empty:
            self.fail("No data arrived!")

        wss.stop()

    def test_subscribing_to_order_book_data_works(self):
        wss = BtfxWss()
        wss.start()
        time.sleep(1)
        wss.subscribe_to_order_book('BTCUSD')
        time.sleep(5)

        try:
            wss.books('BTCUSD').get(block=False)
        except Empty:
            self.fail("No data arrived!")

        wss.stop()

    def test_subscribing_to_raw_book_data_works(self):
        wss = BtfxWss()
        wss.start()
        time.sleep(1)
        wss.subscribe_to_raw_order_book('BTCUSD')
        time.sleep(5)

        try:
            wss.raw_books('BTCUSD').get(block=False)
        except Empty:
            self.fail("No data arrived!")

        wss.stop()

    def test_subscribing_to_trades_data_works(self):
        wss = BtfxWss()
        wss.start()
        time.sleep(1)
        wss.subscribe_to_trades('BTCUSD')
        time.sleep(5)

        try:
            wss.trades('BTCUSD').get(block=False)
        except Empty:
            self.fail("No data arrived!")

        wss.stop()

    def test_subscribing_to_candle_data_works(self):
        wss = BtfxWss()
        wss.start()
        time.sleep(1)
        wss.subscribe_to_candles('BTCUSD')
        time.sleep(5)

        try:
            wss.candles('BTCUSD', '1m').get(block=False)
        except Empty:
            self.fail("No data arrived!")

        wss.stop()

if __name__ == '__main__':
    unittest.main(verbosity=2)