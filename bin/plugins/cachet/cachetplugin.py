import logging
import ConfigParser
from bin.util.constants import ApplicationConstants
from bin.plugins.cachet.cachet import Cachet
from bin.plugins.cachet.cachet import Component
from bin.plugins.cachet.cachet import Metric
from bin.plugins.cachet.cachet import MetricPoint
from bin.plugins.cachet.cachet import Incident


class CachetPlugin:

    def __init__(self):
        self.__cachet = Cachet()
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
        component = self.__cachet.get_component_by_name(endpoint=self.__cachet_host_url, name=component_name)
        incident = Incident(name=component_name + "_incident", message=message, component_id=component.get_id(),
                            visible=1, status=0, component_status=4)
        self.__cachet.create_incident(endpoint=self.__cachet_host_url, incident=incident, token=self.__cachet_token)

    def update_component_status(self, component_name, status):
        component = self.__cachet.get_component_by_name(endpoint=self.__cachet_host_url, name=component_name)
        self.__cachet.update_component_status(endpoint=self.__cachet_host_url, component=component, status=status,
                                              token=self.__cachet_token)

    def get_component_by_name(self, component_name):
        return self.__cachet.get_component_by_name(endpoint=self.__cachet_host_url, name=component_name)

    def create_component_by_name(self, component_name):
        component = Component(name=component_name)
        self.__cachet.create_component(endpoint=self.__cachet_host_url, component=component, token=self.__cachet_token)
        logging.debug("Creating component %s" % component.get_name())

    def get_metric_by_name(self, metric_name):
        return self.__cachet.get_metric_by_name(endpoint=self.__cachet_host_url, name=metric_name)

    def create_metric_by_name(self, metric_name):
        metric = Metric(name=metric_name)
        self.__cachet.create_metric(endpoint=self.__cachet_host_url, metric=metric, token=self.__cachet_token)
        logging.debug("Creating metric %s" % metric.get_name())

    def update_metric_point(self, point, metric_name, timestamp):
        metrics = self.__cachet.get_metrics(endpoint=self.__cachet_host_url)
        for current_metric in metrics:
            if current_metric.get_name() == metric_name:
                metric_point = MetricPoint(current_metric.get_id(), point, int(timestamp))
                self.__cachet.create_metric_point(endpoint=self.__cachet_host_url, metric_point=metric_point,
                                                  token=self.__cachet_token)
