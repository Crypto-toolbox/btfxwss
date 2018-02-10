from unittest import mock
from socket import socket
from threading import Event, Timer
import pytest
import zmq
import logging
import ssl
import json
import hashlib
import websocket

from btfxwss.zmq.connection import WebSocketConnection


@pytest.fixture
def FakeWebSocketConnection():
    conn = WebSocketConnection()
    conn.connected = mock.MagicMock(spec=Event)
    conn.reconnect_required = mock.MagicMock(spec=Event)
    conn.disconnect_called = mock.MagicMock(spec=Event)
    conn.paused = mock.MagicMock(spec=Event)
    conn.ctx = mock.MagicMock(spec=zmq.Context)
    conn.socket = mock.MagicMock(spec=socket)
    conn.log = mock.MagicMock(spec=logging.Logger)
    return conn


def test_channel_property():
    conn = WebSocketConnection()
    conn._channels = {1: 'hello'}
    assert conn.channels == ['hello']


def test_reconnect_call():
    conn = WebSocketConnection()
    fake_socket = mock.MagicMock(spec=socket)
    conn.socket = fake_socket
    conn.connected.set()

    conn.reconnect()
    assert fake_socket.close.called
    assert conn.reconnect_required.is_set()
    assert not conn.connected.is_set()


def test_disconnect_call():
    conn = WebSocketConnection()
    fake_socket = mock.MagicMock(spec=socket)
    conn.socket = fake_socket

    with mock.patch.object(conn, 'join') as fake_join:
        conn.disconnect()
        assert fake_join.called
        assert fake_socket.close.called
        assert not conn.reconnect_required.is_set()
        assert not conn.connected.is_set()
        assert conn.disconnect_called.is_set()


def test_send_ping():
    with mock.patch('threading.Timer') as mock_timer:
        conn = WebSocketConnection()
        conn.socket = mock.MagicMock(spec=socket)
        conn.send_ping()

        conn.socket.assert_called_with('{"event":"ping"}')
        mock_timer.assert_called_once_with(30, conn._check_pong)
        assert conn.pong_timer == mock_timer
        assert mock_timer.start.called


def test_run():
    conn = WebSocketConnection()
    with mock.patch.object(conn, '_connect') as mock_connect:
        conn.run()
        assert mock_connect.called


def test_on_message():
    pytest.fail("Finish this test!")


def test_on_close():
    conn = WebSocketConnection()
    fake_event = mock.MagicMock(spec=Event)
    conn.connected = fake_event
    with mock.patch.object(conn, '_stop_timers') as mock_stop_timers:
        conn._on_close(None)
        assert mock_stop_timers.called
        assert fake_event.clear.called


def test_on_open(FakeWebSocketConnection):
    conn = FakeWebSocketConnection

    with mock.patch.object(conn, '_start_timers') as fake_start_timers:
        with mock.patch.object(conn, '_subscribe') as fake_subscribe:
            with mock.patch.object(conn, 'send_ping') as fake_send_ping:
                conn._on_open(None)

                assert conn.log.info.called
                assert conn.connected.set.called
                assert conn.send_ping.called
                assert conn._start_timers.called
                assert conn._subscribe.called


def test_on_error(FakeWebSocketConnection):
    conn = FakeWebSocketConnection
    conn._on_error(None, 'Error')
    assert conn.log.info.called
    assert conn.reconnect_required.set.called
    assert conn.connected.clear.called


@mock.patch('websocket.WebSocketApp')
def test_internal_connect(FakeWebSocketConnection, *args):
    fake_websocket_app, *_ = args
    conn = FakeWebSocketConnection
    conn.publisher = mock.MagicMock(spec=socket)
    conn.reconnect_required.is_set.return_value = False
    conn._connect()
    assert fake_websocket_app.assert_called_once_with(
        conn.url, on_open=conn._on_open, on_message=conn._on_message, on_error=conn._on_error,
        on_close=conn._on_close
    )
    assert conn.socket.run_forever.assert_called_with(
        sslopt={'ca_certs': ssl.get_default_verify_paths().ca_certs}
    )
    conn.log.debug.assert_called_with('_connect(): Starting Connection..')
    assert conn.socket.close.called
    assert conn.publisher.close.called
    assert conn.ctx.destroy.called


def test_subscribe():
    pytest.fail("Finish this test!")


def test_stop_timers(FakeWebSocketConnection):
    conn = FakeWebSocketConnection
    conn.ping_timer = mock.MagicMock(spec=Timer)
    conn.connection_timer = mock.MagicMock(spec=Timer)
    conn.pong_timer = mock.MagicMock(spec=Timer)

    conn._stop_timers()
    assert conn.ping_timer.cancel.called
    assert conn.connection_timer.cancel.called
    assert conn.pong_timer.cancel.called
    conn.log.debug.assert_called_with("_stop_timers(): Timers stopped.")


@mock.patch('threading.Timer')
def test_start_timers(FakeWebSocketConnection, *args):
    fake_timer, *_ = args
    conn = FakeWebSocketConnection
    conn._start_timers()
    with mock.patch.object(conn, '_stop_timers') as fake_stop_timers:
        conn.log.debug.assert_called_with('_start_timers(): Resetting timers..')
        fake_stop_timers.assert_called_once()
        fake_timer.assert_called_with(conn.ping_interval, conn.send_ping)
        fake_timer.assert_called_with(conn.connection_timeout, conn._connection_timed_out)
        assert fake_timer.start.call_count == 2


def test_check_pong(FakeWebSocketConnection, ):
    conn = FakeWebSocketConnection
    conn.pong_timer = mock.MagicMock(spec=Timer)

    conn.pong_received = True
    conn._check_pong()
    conn.log.debug.assert_called_with('_check_pong(): Pong received in time.')
    assert conn.pong_received == False

    with mock.patch.object(conn, 'reconnect') as fake_reconnect:
        conn._check_pong()
        assert fake_reconnect.called
        conn.log.debug.assert_called_with("_check_pong(): Pong not received in time. "
                                          "Issuing reconnect..")


@mock.patch('hmac.new', return_value='ok')
@mock.patch('hmac.HMAC.hexdigest', return_value='ok')
def test_send(*args, FakeWebSocketConnection):
    fake_hmac_new, fake_hmac_hexdigest, *_ = args
    expected_auth_payload = {'event': 'auth', 'apiKey': 'api_key', 'authSig': 'ok',
                             'authPayload': 'AUTH1000000', 'authNonce': '1000000'}
    expected_kwargs_payload = ['this', 'is', 'a', 'list']
    expected_list_data_payload = {'pair': 'BTC-USD', 'price': '10.0', 'size': '1.0'}

    conn = FakeWebSocketConnection
    with mock.patch('time.time', return_value=1):
        json_payload = conn._send(api_key='api_key', secret='secret', auth=True)
        assert json.loads(json_payload) == expected_auth_payload
        fake_hmac_new.assert_called_with('secret'.encode(), 'AUTH1000000'.encode(), hashlib.sha384)
        fake_hmac_hexdigest.assert_called_with('ok')

    json_payload = conn._send(list_data=['this', 'is', 'a', 'list'])
    assert json.loads(json_payload) == expected_list_data_payload

    json_payload = conn._send(pair="BTC-USD", price="10.0", size="1.0")
    assert json.loads(json_payload) == expected_kwargs_payload

    assert conn.socket.send.call_count == 3
    conn.socket.send.assert_called_with(json.dumps(expected_auth_payload))
    conn.socket.send.assert_called_with(json.dumps(expected_list_data_payload))
    conn.socket.send.assert_called_with(json.dumps(expected_kwargs_payload))

    assert conn.log.debug.call_count == 3

    conn.socket.send.return_value = websocket.WebSocketConnectionClosedException()
    conn.send({})
    conn.log.error.assert_called_with(
        '_send(): Did not send out payload %s - client not connected.' % {}
    )


def test_publish(FakeWebSocketConnection):
    conn = FakeWebSocketConnection
    conn._channels = {1: 'Test', 0: 'Account'}
    conn.publisher = mock.MagicMock(spec=zmq.Socket)
    test_data = ['apple', 1, 2, 'Banana']
    ts = 23

    # Test public channel publishing
    expected_frames = [json.dumps(x.encode()) for x in ('Test', test_data, ts)]
    conn.publish(1, test_data, ts)
    conn.log.info.assert_called_with(
        "publish(): Sending frames %s from address %s..", expected_frames, conn.zmq_addr)
    conn.publisher.send_multipart.assert_called_with(expected_frames)

    # Test account channel publishing
    expected_frames = [json.dumps(x.encode()) for x in ('Account/apple', test_data, ts)]
    conn.publish(0, test_data, ts)
    conn.log.info.assert_called_with(
        "publish(): Sending frames %s from address %s..", expected_frames, conn.zmq_addr)
    conn.publisher.send_multipart.assert_called_with(expected_frames)


def test_connection_timed_out(FakeWebSocketConnection):
    conn = FakeWebSocketConnection
    with mock.patch.object(conn, 'reconnect') as fake_reconnect:
        conn._connection_timed_out()
        conn.log.debug.assert_called_with('_connection_timed_out(): Fired! Issuing reconnect..')
        assert fake_reconnect.called


def test_system_handler():
    pytest.fail("Finish this test!")


def test_heartbeat_handler(FakeWebSocketConnection):
    conn = FakeWebSocketConnection
    with mock.patch.object(conn, '_start_timers') as fake_start_timers:
        conn.log.debug.assert_called_with(
            "_heartbeat_handler(): Received a heart beat from connection!")
        assert fake_start_timers.called


def test_subscription_handler():
    pytest.fail("Finish this test!")


def test_handle_ERR(FakeWebSocketConnection):
    conn = FakeWebSocketConnection
    conn._handle_ERR({'code': 123, 'msg': 'test message'}, None)
    conn.log.error.assert_called_with("[ERROR] %s: %s", 123, 'test message')


def test_handle_EVT(FakeWebSocketConnection):
    conn = FakeWebSocketConnection

    # Test code 20051
    d = {'code': 20051}
    conn._handle_EVT(d, 123)
    conn.log.info.assert_called_with("[EVT_STOP] - WebSocket Server stopping..")
    assert conn.paused.set.called

    # Test code 20060
    d['code'] = 20060
    conn._handle_EVT(d, 123)
    conn.log.info.assert_called_with("[EVT_RESYNC_START] - Websocket Server syncing...")

    # Test code 20061
    d['code'] = 20061
    conn._handle_EVT(d, 123)
    conn.log.info.assert_called_with("[EVT_RESYNC_STOP] - WebSocket Server sync complete.")
    assert conn.paused.clear.called

    # Test Invalid Code 99999
    d['code'] = 99999
    conn._handle_EVT(d, 123)
    conn.log.error.assert_called_with("Unhandled ERR message %s", d)


def test_data_handler(FakeWebSocketConnection):
    conn = FakeWebSocketConnection
    with mock.patch.object(conn, 'publish') as fake_publish:
        conn._data_handler([1,2,3], 123)
        fake_publish.assert_called_with(1, [2,3], 123)


def test_prep_auth_payload(FakeWebSocketConnection):
    conn = FakeWebSocketConnection
    assert conn._prep_auth_payload('channel', 'hello') == [0, 'channel', None, 'hello']


def test_input_methods(FakeWebSocketConnection):
    conn = FakeWebSocketConnection
    with mock.patch.object(conn, '_send') as fake_send:
        conn.new_order(option_1=True)
        fake_send.assert_called_with(list_data=[0, 'on', None, {'option_1': True}])

        conn.cancel_order(option_1=True)
        fake_send.assert_called_with(list_data=[0, 'oc', None, {'option_1': True}])

        conn.cancel_multi_orders(option_1=True)
        fake_send.assert_called_with(list_data=[0, 'oc_multi', None, {'option_1': True}])

        conn.multi_op_orders(option_1=True)
        fake_send.assert_called_with(list_data=[0, 'ox_multi', None, {'option_1': True}])

        conn.calc(option_1=True)
        fake_send.assert_called_with(list_data=[0, 'calc', None, {'option_1': True}])
