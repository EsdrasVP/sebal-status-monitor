import os
import random
import logging
import ConfigParser

from openstack import metric

from bin.util.constants import ApplicationConstants
from bin.plugins.cachet.cachet import Cachet
from bin.plugins.cachet.cachet import MetricPoint
from bin.plugins.cachet.cachet import Incident


class CachetPlugin:

    def __init__(self):
        self.__config = ConfigParser.ConfigParser()
        self.__config.read(ApplicationConstants.DEFAULT_CONFIG_FILE_PATH)
        self.__cachet_host_url = self.config_section_map("SectionThree")['cachet_host_url']
        self.__cachet_token = self.config_section_map("SectionThree")['cachet_token']
        self.__cachet_api = Cachet.__init__()

    def config_section_map(self, section):
        dict1 = {}
        options = self.__config.options(section)
        for option in options:
            try:
                dict1[option] = self.__config.get(section, option)
                if dict1[option] == -1:
                    logging.debug("skip: %s", option)
            except Exception as e:
                logging.debug(str(e))
                dict1[option] = None
        return dict1

    def update_image_number_cachet(self, number_of_images, state, timestamp):
        endpoint = os.path.join(self.__cachet_host_url, ApplicationConstants.METRICS_ENDPOINT)
        metrics = self.__cachet_api.get_metrics(endpoint)
        for current_metric in metrics:
            if state in current_metric.get_name():
                metric_point = MetricPoint.__init__(current_metric.get_id(), number_of_images, timestamp)
                self.__cachet_api.create_metric_point(endpoint, metric_point, self.__cachet_token)

    def set_incident(self):
        # TODO: implement
        return None

    def set_operation_failure(self, component_name, message):
        # TODO: implement
        # Here, we will register an operation failure and an incident. So it might change to receive a message based on
        # the failure for we to know, automatically, which failure caused the incident.
        component = self.__cachet_api.get_component_by_name(component_name)
        incident = Incident.__init__(random.seed(a=int))
        self.__cachet_api.create_incident('endpoint', incident, self.__cachet_token)
        return None
