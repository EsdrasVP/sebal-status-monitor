import logging
import ConfigParser
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

    def set_operation_failure(self, component_name, message):
        # TODO: implement
        # Here, we will register an operation failure and an incident. So it might change to receive a message based on
        # the failure for we to know, automatically, which failure caused the incident.
        component = Cachet.get_component_by_name(endpoint=self.__cachet_host_url, name=component_name)
        incident = Incident.__init__(message=message, component_id=component.get_id(), component_status=4)
        Cachet.create_incident(endpoint=self.__cachet_host_url, incident=incident, token=self.__cachet_token)

    def update_component_status(self, component_name, status):
        component = Cachet.get_component_by_name(endpoint=self.__cachet_host_url, name=component_name)
        Cachet.update_component_status(endpoint=self.__cachet_host_url, component=component, status=status,
                                       token=self.__cachet_token)

    def update_image_number(self, number_of_images, metric_name, timestamp):
        metrics = Cachet.get_metrics(endpoint=self.__cachet_host_url)
        for current_metric in metrics:
            if current_metric.get_name() == metric_name:
                metric_point = MetricPoint.__init__(current_metric.get_id(), number_of_images, timestamp)
                Cachet.create_metric_point(endpoint=self.__cachet_host_url, metric_point=metric_point,
                                           token=self.__cachet_token)

    def update_average_image_execution_time(self, image_execution_time, timestamp):
        metrics = Cachet.get_metrics(endpoint=self.__cachet_host_url)
        for current_metric in metrics:
            if current_metric.get_name == ApplicationConstants.AVG_EXECUTION_TIME_METRIC_NAME:
                metric_point = MetricPoint.__init__(current_metric.get_id(), image_execution_time, timestamp)
                Cachet.create_metric_point(endpoint=self.__cachet_host_url, metric_point=metric_point,
                                           token=self.__cachet_token)
