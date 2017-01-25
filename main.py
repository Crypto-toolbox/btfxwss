# Import Built-Ins
import logging
import time
import sys

# Import Third-Party

# Import Homebrew
from btfxwss import BtfxWss
# Init Logging Facilities
logging.basicConfig(level=logging.DEBUG, filename='test.log')
log = logging.getLogger(__name__)

fh = logging.FileHandler('test.log')
fh.setLevel(logging.DEBUG)
sh = logging.StreamHandler(sys.stdout)
sh.setLevel(logging.DEBUG)

log.addHandler(sh)
log.addHandler(fh)


if __name__ == '__main__':
    wss = BtfxWss()
    wss.start()
    time.sleep(1)
    wss.ticker('BTCUSD')
    wss.ping()
    t = time.time()
    while time.time() - t < 10:
        time.sleep(1)
    wss.stop()
