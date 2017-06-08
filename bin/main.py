import time
from bin.monitor.monitor import Monitor
from bin.util.constants import ApplicationConstants


def init():
    status_monitor = Monitor()
    while True:
        status_monitor.start()
        time.sleep(ApplicationConstants.DEFAULT_MONITOR_SLEEP_TIME)


init()
