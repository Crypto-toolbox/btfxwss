from threading import Thread, Event


class Channel(Thread):
    def __init__(self, channel_name, event_q, *args, **kwargs):
        self.name = channel_name
        self.event_q = event_q
        self.event_callbacks = {}
        self._listening = Event()
        self._stopped = Event()
        super(Channel, self).__init__(*args, **kwargs)

    def put(self, data, **kwargs):
        self.event_q.put(data, **kwargs)

    def get(self, **kwargs):
        self.event_q.get(**kwargs)

    def _handle_event(self, event_name, data):
        if event_name in self.event_callbacks.keys():
            for callback in self.event_callbacks[event_name]:
                callback(data)

    def join(self, *args, **kwargs):
        self._stopped.set()
        super(Channel, self).join(*args, **kwargs)

    def listen(self):
        """Sets the internal _listening Event to true, so Main Loop runs
        code"""
        self._listening.set()

    def pause(self):
        """Sets the internal _listening event to False, so Main loop doesn't
        run code, but is on standby"""
        self._listening.clear()

    def run(self):
        while not self._stopped.is_set():
            try:
                while self._listening.wait(timeout=0.1):
                    pass
            except TimeoutError:
                continue


class TickerChannel(Channel):
    pass


class BookChannel(Channel):
    pass


class RawBookChannel(Channel):
    pass


class TradesChannel(Channel):
    pass


class CandlesChannel(Channel):
    pass
