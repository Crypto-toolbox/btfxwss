# 1.1.0

## Added
- Support for Authentication Channels Data Stream
- Properties on Client class to ease access to account information

# 1.0.2

## Fixed
- Fixed an issue where identifier in the queue processor's `channel_directory` attribute would store incorrect keys for candle data.

# 1.0

## Added
- Changelog
- Proper Branching model
- Semantic versioning enforced

## Changed
- Moved processing of data and connection handling to separate class each
- rewrote connection code
- Data is now supplied as queues
- Removed `open()` call in setup.py, caused error on install


## Removed
