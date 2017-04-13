from bin.monitor.monitor import Monitor


def init():
    status_monitor = Monitor.__init__()
    status_monitor.start()


init()
