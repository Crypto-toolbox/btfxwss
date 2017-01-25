"""
Task:
Descripion of script here.
"""

# Import Built-Ins
import logging
import time

# Import Third-Party

# Import Homebrew
from btfxwss import BtfxWss
# Init Logging Facilities
log = logging.getLogger(__name__)


if __name__ == '__main__':
    wss = BtfxWss()
    wss.start()
    t = time.time()
    while time.time() - t < 5:
        time.sleep(1)
    wss.stop()