import unittest
from mock import patch, Mock, MagicMock
from bin.monitor.monitor import Monitor


class MonitorTest(unittest.TestCase):
    def setUp(self):
        self.__monitor = Monitor()