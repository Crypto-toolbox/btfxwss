"""Provide Sockets for interfacing with the ZMQ publisher."""
# Import Built-Ins
import logging
import zmq
from queue import Queue
from threading import Thread, Event
import multiprocessing as mp

# Import Homebrew


# Init Logging Facilities
log = logging.getLogger(__name__)


class BtfxWssSocket:
    """Basic ZMQ Socket to interface with the btfxWss publisher."""

    def __init__(self, address, topics=None, ctx=None):
        """Initialize a BtfxWssSocket."""
        self.ctx = ctx or zmq.Context.instance()
        self.topics = topics if topics else b""
        self.sock = None
        self.addr = address

    def start(self):
        """Start the socket."""
        self.sock = self.ctx.socket(zmq.SUB)
        self.sock.setsockopt(zmq.SUBSCRIBE, self.topics)
        self.sock.connect(self.addr)

    def stop(self):
        """Close the socket and kill the context."""
        self.sock.close()
        self.ctx.destroy()

    def recv(self, block=True):
        """Receive a message via the socket.

        If block is set to false, this will resolve instantly - if no message was available, it
        will return a tuple of Nones. Blocks by default.

        :param block: Set blocking of the recv call to True or False
        :return: Three-item tuple of (channel, data, ts) if a message is available,
                 else (None, None, None)
        """
        if block:
            return self.sock.recv_multipart()

        try:
            return self.sock.recv_multipart(zmq.NOBLOCK)
        except zmq.Again:
            return None, None, None


class BtfxWssSocketThread(Thread, BtfxWssSocket):
    """Creates a thread object which returns a Queue() upon calling its start method.

    Data received is fed to this queue and can be read using Queue.get().
    """

    def __init__(self, topics=None, ctx=None, queue=None, **kwargs):
        """Initialize the socket as a thread."""
        super(BtfxWssSocketThread, self).__init__(topics, ctx)
        Thread.__init__(self, **kwargs)
        self.queue = queue or Queue()
        self.running = Event()

    def start(self):
        """Start the socket and thread."""
        self.running.set()
        BtfxWssSocket.start(self)
        Thread.start(self)
        return self.queue

    def stop(self, timeout=None):
        """Stop the socket and thread."""
        self.running.clear()
        self.join(timeout)
        BtfxWssSocket.stop(self)

    def join(self, timeout=None):
        """Join the thread."""
        Thread.join(self, timeout)

    def run(self):
        """Receive data and put it on the queue."""
        while self.running.is_set():
            channel, data, ts = self.recv(block=False)
            if not channel:
                continue
            self.queue.put((channel, data, ts))


class BtfxWssSocketProcess(mp.Process, BtfxWssSocket):
    """Creates a Process object which returns a mp.Queue() upon calling its start method.

    Data received is fed to this queue and can be read using mp.Queue.get().
    """

    def __init__(self, topics=None, ctx=None, queue=None, **kwargs):
        """Initialize the socket as a process."""
        super(BtfxWssSocketProcess, self).__init__(topics, ctx)
        mp.Process.__init__(self, **kwargs)
        self.queue = queue or mp.Queue()
        self.running = mp.Event()

    def start(self):
        """Start the socket and process."""
        self.running.set()
        BtfxWssSocket.start(self)
        mp.Process.start(self)
        return self.queue

    def stop(self, timeout=None):
        """Stop the socket and process."""
        self.running.clear()
        self.join(timeout)
        BtfxWssSocket.stop(self)

    def join(self, timeout=None):
        """Join the process."""
        mp.Process.join(self, timeout)

    def run(self):
        """Receive data and put it on the queue."""
        while self.running.is_set():
            channel, data, ts = self.recv(block=False)
            if not channel:
                continue
            self.queue.put((channel, data, ts))
