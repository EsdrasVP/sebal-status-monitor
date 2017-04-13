from bin.monitor.monitor import Monitor
import time


DEFAULT_MONITOR_SLEEP_TIME = 3600


def init():
    status_monitor = Monitor.__init__()
    while True:
        status_monitor.start()
        time.sleep(DEFAULT_MONITOR_SLEEP_TIME)


init()
