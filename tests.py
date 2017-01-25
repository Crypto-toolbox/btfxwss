# Import Built-Ins
import logging
import unittest
from contextlib import redirect_stdout
from io import StringIO
# Import Third-Party

# Import Homebrew
from btfxwss.classes import BtfxWss
# import Server-side Exceptions
from btfxwss.exceptions import RestartServiceInfo, PauseWSSClientInfo
from btfxwss.exceptions import UnpauseWSSClientInfo, GenericSubscriptionError
from btfxwss.exceptions import NotSubscribedError,  AlreadySubscribedError
from btfxwss.exceptions import InvalidPairError, InvalidChannelError
from btfxwss.exceptions import InvalidEventError
# import Client-side Exceptions
from btfxwss.exceptions import UnknownEventError, UnknownWSSError
from btfxwss.exceptions import UnknownWSSInfo, AlreadyRegisteredError
from btfxwss.exceptions import NotRegisteredError, UnknownChannelError

# Init Logging Facilities
log = logging.getLogger(__name__)


class BtfxWssTests(unittest.TestCase):

    def test_handle_response_raises_exc_on_unknown_event(self):
        wss = BtfxWss()
        dummy_response_faulty = {'event': 'unknown event'}
        wss._event_handlers['test_event'] = True
        with self.assertRaises(UnknownEventError):
            wss.handle_response(dummy_response_faulty)

    def test_handle_response_takes_care_of_serverside_errors_gracefully(self):
        """
        Any BtfxServerErrors should be handled gracefully, and the flow of
         the program should not be interrupted. Any information gained
        from these Errors (for example responses to executed commands),
        should be printed to stdout.
        """
        func_output = StringIO()
        with redirect_stdout(func_output):
            pass

        self.fail("Not Implemented!")

    def test_handle_subscribed_registers_new_channel_correctly(self):
        wss = BtfxWss()
        dummy_response = {'event': 'subscribed', 'chanId': 5,
                          'channel': 'book'}

        wss._handle_subscribed(**dummy_response)
        self.assertIn(5, wss.channels)

        # Check if UnknownChannelError is raised if a channel not present
        # in Btfx._data_handlers is submitted for registration
        faulty_dummy_response = {'event': 'subscribed', 'chanId': 6,
                                 'channel': 'Not a channel'}

        with self.assertRaises(UnknownChannelError):
            wss._handle_subscribed(**faulty_dummy_response)

        # Check if AlreadyRegisteredError is raised when a channel is
        # submitted for registration again (causing the old associated id
        # to be overwritten)
        dummy_response = {'event':   'subscribed', 'chanId': 5,
                          'channel': 'book'}
        with self.assertRaises(AlreadyRegisteredError):
            wss._handle_subscribed(**dummy_response)

        self.assertEqual({5: wss._handle_book}, wss.channels)

    def test_handle_unsubscribed_removes_channels_correctly(self):
        # Set up a few things first
        wss = BtfxWss()
        dummy_response = {'event': 'subscribed', 'chanId': 5,
                          'channel': 'book'}

        # fill Btfx.channels with a test channel
        wss._handle_subscribed(**dummy_response)
        self.assertIn(5, wss.channels)

        # Check that registered channels are removed properly
        dummy_response = {'event':   'subscribed', 'chanId': 5,
                          'channel': 'book'}
        wss._handle_unsubscribed(**dummy_response)
        self.assertEqual({}, wss.channels)

        # Check if NotRegisteredError is raised if a channel not present
        # in Btfx.channels is submitted for unregistration
        faulty_dummy_response = {'event': 'unsubscribed', 'chanId': 6,
                                 'channel': 'book'}

        with self.assertRaises(NotRegisteredError):
            wss._handle_unsubscribed(**faulty_dummy_response)

    def test_handle_error_raises_exceptions_correctly(self):
        wss = BtfxWss()

        # Check if valid code is retrieved from BtfxWss._code_handlers
        # correctly, raising it properly.
        dummy_response = {'event': 'error', 'code': 10300,
                          'channel': 'book'}
        with self.assertRaises(GenericSubscriptionError):
            wss._handle_error(**dummy_response)

        dummy_response = {'event': 'error', 'code': 100,
                          'channel': 'book'}
        with self.assertRaises(UnknownWSSError):
            wss._handle_error(**dummy_response)

    def test_handle_info_runs_proper_funcs_correctly(self):
        wss = BtfxWss()

        # Check if valid code is retrieved from BtfxWss._code_handlers
        # correctly, executing the function properly.
        dummy_response = {'event': 'info', 'code': 20060,
                          'msg': 'PAUSE, mathafackaa!'}
        wss._handle_info(**dummy_response)
        self.assertEqual(wss._paused, True)

        dummy_response = {'event': 'info', 'code': 20061,
                          'msg': 'Unpause, motherfackaaa!'}
        wss._handle_info(**dummy_response)
        self.assertEqual(wss._paused, False)

        # Check that ValueError is raised when an Info Code not starting
        # with '2' is passed to _handle_info()
        dummy_response = {'event': 'info', 'code': 10300,
                          'channel': 'book'}
        with self.assertRaises(ValueError):
            wss._handle_info(**dummy_response)

        # Check that UnknownWSSInfo is raised when an Info code not found
        # in BtfxWss._code_handlers is passed to _handle_info().
        dummy_response = {'event': 'info', 'code': 2000,
                          'channel': 'book'}
        with self.assertRaises(UnknownWSSInfo):
            wss._handle_info(**dummy_response)

    def test_handle_data_outputs_data_as_expected(self):
        # Prepare BtfxWss Instance for testing
        wss = BtfxWss()
        for i, channel in enumerate(['ticker', 'book', 'raw_book', 'trades', 'candles']):
            dummy_resp = {'channel': channel, 'chanId': i, 'event': 'subscribed'}
            wss.handle_response(dummy_resp)

        # Test that BtfxWss._handle_book() properly handles snapshots and updates
        dummy_book = [[i+1000, i, i] for i in range(50)] + [[i+1000, i, i] for i in range(50)]
        dummy_data_snapshot = [1, dummy_book]
        dummy_data_update = [1, (dummy_book[0][0], 0, dummy_book[0][2])]

        expected_result_snapshot = ''
        expected_result_update = ''

        func_output = StringIO()
        with redirect_stdout(func_output):
            wss.handle_data(dummy_data_snapshot)

        self.assertEqual(func_output, expected_result_snapshot)
        func_output = StringIO()
        with redirect_stdout(func_output):
            wss.handle_data(dummy_data_update)

        self.assertEqual(func_output, expected_result_update)
        self.fail('Finish the test!')


