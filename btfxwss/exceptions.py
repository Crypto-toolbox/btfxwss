"""
Contains Exceptions (Errors, Warnings, Infos) relevant to BtfxWss.

Generally, BtfxServerInfo & BtfxServerError Exceptions should
be handled.

BtfxClientErrors should be raised.

"""
# Import Built-Ins
import logging

# Init Logging Facilities
log = logging.getLogger(__name__)


class BtfxServerInfo(Exception):
    """
    General Info exception for status codes sent by the BTFX server
    (field 'code' in payload and 'event' == 'info').
    """


class RestartServiceInfo(BtfxServerInfo):
    """
    The server has issued us to restart / reconnect to the websocket service.
    """


class PauseWSSClientInfo(BtfxServerInfo):
    """
    The Server has issued a pause on activity, due to the trading engine being
    refreshed (according to docs@12.01.18, this takes around 10s).
    """


class UnpauseWSSClientInfo(BtfxServerInfo):
    """
    The Server has given greenlight to continue activity.
    """


class BtfxServerError(Exception):
    """
    General Error for any status codes sent by bitfinex websockets
    ('event' field of value 'info' or 'error').

    These should be handled and activity should continue as normal.
    """


class GenericSubscriptionError(BtfxServerError):
    """
    A generic Error occurred during the subscription process - check your
    payload and consult with BTFX support.
    """


class AlreadySubscribedError(BtfxServerError):
    """
    The subscription existed already.
    """


class NotSubscribedError(BtfxServerError):
    """
    You were not subscribed to the channel you've tried unsubscribing from.
    """


class InvalidEventError(BtfxServerError):
    """
    Raised when an unknown event is received via the websocket ('event' field in
    payload).
    """


class InvalidPairError(BtfxServerError):
    """
    Raised when an unknown pair error code is received via the websocket.
    (field 'code' in payload)
    """


class InvalidChannelError(BtfxServerError):
    """
    Raised when an unknown channel error code is received via the websocket.
    (field 'code' in payload, 'event' == 'error')
    """


class InvalidBookPrecisionError(BtfxServerError):
    """
    Raised when subscribing to the book channel and passing an invalid
    book precision parameter.
    """


class InvalidBookLengthError(BtfxServerError):
    """
    Raised when subscribing to the book channel and passing an invalid
    book length parameter.
    """


class BtfxClientError(Exception):
    """
    General class for BtfxWss Client-related errors. This includes errors raised due
    to unknown events, status codes, and book-keeping errors (i.e. duplicate
    subscriptions and the like).

    These should be raised, as they indicate an error in code and function,
    possibly leading to faulty data.
    """


class UnknownEventError(BtfxClientError):
    """
    Raised when an unknown event is received via the websocket ('event' field in
    payload).
    """


class UnknownWSSError(BtfxClientError):
    """
    Raised when an unknown error code is received via the websocket.
    (field 'code' in payload, 'event' == 'error')
    """


class UnknownChannelError(BtfxClientError):
    """
    Raised when a channel is received via the websocket for which BtfxWss has
    no data handler set in _data_handlers.
    (field 'channel' in payload, 'event' == 'subscribed' | 'unsubscribed')
    """


class UnknownWSSInfo(BtfxClientError):
    """
    Raised when an unknown info code is received via the websocket.
    (field 'code' in payload, 'event' == 'info')
    """


class AlreadyRegisteredError(BtfxClientError):
    """
    Raised when a subscription response is received, but the channel ID has
    already been registered in BtfxWss.channels attribute;
    """


class NotRegisteredError(BtfxClientError):
    """
    Raised when data is received, but its channel ID has not been registered
    with the client, or the user tries unsubscribing from a channel that is
    not regustered with the client.
    """