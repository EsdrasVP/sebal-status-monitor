from bin.monitor.monitor import Monitor
from bin.util.constants import ApplicationConstants
import time


def init():
    status_monitor = Monitor.__init__()
    while True:
        status_monitor.start()
        time.sleep(ApplicationConstants.DEFAULT_MONITOR_SLEEP_TIME)


init()
