# 1.1.4

## Fixed
- If a connection drops, the client automatically resubscribes to channels
- Fixed an issue where the candles channel wasn't subscribable due to an overvlap
in variable naming
- Fixed example in README.md
- Added build state in README
- Added option to pass custom SSL ops to websocket
## Changed
- prioritizing symbol over pair now, when creating channel identifier


# 1.1.1

## Added
- Support for Authentication Channels Data Stream
- Properties on Client class to ease access to account information
- More debug log calls to connection class to provide more in-depth information.

## Fixed
- Issue where Error codes would cause a reconnect when not necessary
- Added Tests to verify basic functionality
- Added is_connected decorator to prevent crashing of program upon trying
to call a client method before the connection was established
- fixed `channel_id queried` via `channel_name`, instead of identifier in `Client._unsubscribe()`


# 1.0.2

## Fixed
- Fixed an issue where identifier in the queue processor's `channel_directory` attribute would store incorrect keys for candle data. 
- Fixed issue #18


# 1.0

## Added
- Changelog
- Proper Branching model
- Semantic versioning enforced

## Changed
- Moved processing of data and connection handling to separate class each
- Rewrote connection code
- Data is now supplied as queues
- Removed `open()` call in setup.py, caused error on install

