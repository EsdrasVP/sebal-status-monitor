#!/usr/bin/python
import json
import logging
import sys
import os.path
from bin.plugins.cachet.cachet_api_v1 import CachetApiV1

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

CONST_DATA_JSON_ATTR = "data"
CONST_NAME_JSON_ATTR = "name"
CONST_ID_JSON_ATTR = "id"

CONST_ERROR_MESSAGE_CACHET_API_V1 = "CachetApiV1 Error."

logger = logging.getLogger(__name__)


class Cachet:
    def __init__(self):
        '''__init__ Constructor'''
        pass

    # FIXME attributes name
    @staticmethod
    def get_components(endpoint):
        json_str = CachetApiV1.get_components(endpoint)
        if json_str is None:
            raise Exception(CONST_ERROR_MESSAGE_CACHET_API_V1)

        json_object = json.loads(json_str)
        components_json_array = json_object[CONST_DATA_JSON_ATTR]
        components = []
        for component_json in components_json_array:
            name = component_json[CONST_NAME_JSON_ATTR]
            id = component_json[CONST_ID_JSON_ATTR]
            components.append(Component(id=id, name=name))
        return components

    def get_component_by_name(self, endpoint, name):
        components = self.get_components(endpoint)
        if components is None or len(components) == 0:
            return None
        for component in components:
            if component.get_name() == name:
                return component
        return None

    @staticmethod
    def create_component(endpoint, component, token):
        json_str = CachetApiV1.create_component(endpoint, component.get_name(), component.get_status(),
                                                component.get_group_id(), token)
        if json_str is None:
            raise Exception(CONST_ERROR_MESSAGE_CACHET_API_V1)

        json_object = json.loads(json_str)
        component_json = json_object[CONST_DATA_JSON_ATTR]
        id = component_json[CONST_ID_JSON_ATTR]
        name = component_json[CONST_NAME_JSON_ATTR]
        return Component(id=id, name=name)

    @staticmethod
    def update_component_status(endpoint, component, status, token):
        json_str = CachetApiV1.update_component_status(endpoint, component.get_name(), status,
                                                       component.get_group_id(), token)
        if json_str is None:
            raise Exception(CONST_ERROR_MESSAGE_CACHET_API_V1)

        json_object = json.loads(json_str)
        component_json = json_object[CONST_DATA_JSON_ATTR]
        id = component_json[CONST_ID_JSON_ATTR]
        name = component_json[CONST_NAME_JSON_ATTR]
        return Component(id=id, name=name)

    @staticmethod
    def get_groups_component(endpoint):
        json_str = CachetApiV1.get_groups_component(endpoint)
        if json_str is None:
            raise Exception(CONST_ERROR_MESSAGE_CACHET_API_V1)

        json_object = json.loads(json_str)
        group_components_json_array = json_object[CONST_DATA_JSON_ATTR]
        groups_components = []
        for group_component_json in group_components_json_array:
            name = group_component_json[CONST_NAME_JSON_ATTR]
            id = group_component_json[CONST_ID_JSON_ATTR]
            groups_components.append(GroupComponent(id, name))
        return groups_components

    def get_group_component_by_name(self, endpoint, name):
        group_components = self.get_groups_component(endpoint)
        if group_components is None or len(group_components) == 0:
            return None
        for group_component in group_components:
            if group_component.get_name() == name:
                return group_component
        return None

    @staticmethod
    def create_group_component(endpoint, group_component, token):
        json_str = CachetApiV1.create_group_component(endpoint, group_component.get_name(), token)
        if json_str is None:
            raise Exception(CONST_ERROR_MESSAGE_CACHET_API_V1)

        json_object = json.loads(json_str)
        group_component_json = json_object[CONST_DATA_JSON_ATTR]
        id = group_component_json[CONST_ID_JSON_ATTR]
        name = group_component_json[CONST_NAME_JSON_ATTR]
        return Component(id=id, name=name)

    @staticmethod
    # FIXME: id shouldn't be generated inside incident's __init?
    def create_incident(endpoint, incident, token):
        json_str = CachetApiV1.create_intent(endpoint, incident.get_name(), incident.get_message(),
                                             incident.get_status(), incident.get_visible(), incident.get_component_id(),
                                             incident.get_component_status(), token)
        if json_str is None:
            raise Exception(CONST_ERROR_MESSAGE_CACHET_API_V1)

        json_object = json.loads(json_str)
        incident_json = json_object[CONST_DATA_JSON_ATTR]
        id = incident_json[CONST_ID_JSON_ATTR]
        name = incident_json[CONST_NAME_JSON_ATTR]
        return Incident(id=id, name=name)

    @staticmethod
    def get_metrics(endpoint):
        json_str = CachetApiV1.get_metrics(endpoint)
        if json_str is None:
            raise Exception(CONST_ERROR_MESSAGE_CACHET_API_V1)

        json_object = json.loads(json_str)
        metrics_json_array = json_object[CONST_DATA_JSON_ATTR]
        metrics = []
        for metric_json in metrics_json_array:
            name = metric_json[CONST_NAME_JSON_ATTR]
            id = metric_json[CONST_ID_JSON_ATTR]
            metrics.append(Metric(id=id, name=name))
        return metrics

    @staticmethod
    def create_metric(endpoint, metric, token):
        json_str = CachetApiV1.create_metric(endpoint, metric.get_name(), metric.get_suffix(), metric.get_description(),
                                             metric.get_default_value(), token)
        if json_str is None:
            raise Exception(CONST_ERROR_MESSAGE_CACHET_API_V1)

        json_object = json.loads(json_str)
        metric_json = json_object[CONST_DATA_JSON_ATTR]
        id = metric_json[CONST_ID_JSON_ATTR]
        name = metric_json[CONST_NAME_JSON_ATTR]
        return Metric(id=id, name=name)

    @staticmethod
    def create_metric_point(endpoint, metric_point, token):
        json_str = CachetApiV1.create_metric_point(endpoint, metric_point.get_metric_id(), metric_point.get_value(),
                                                   metric_point.get_timestamp(), token)
        if json_str is None:
            raise Exception(CONST_ERROR_MESSAGE_CACHET_API_V1)

        json_object = json.loads(json_str)
        metric_point_json = json_object[CONST_DATA_JSON_ATTR]
        id = metric_point_json[CONST_ID_JSON_ATTR]
        return Metric(id=id)


# TODO or FIXME move to other document ?
class Component:
    def __init__(self, id=None, name=None, status=None, group_id=None):
        self.__id = id
        self.__name = name
        self.__status = status
        self.__group_id = group_id
        pass

    def get_id(self):
        return self.__id

    def get_name(self):
        return self.__name

    def get_status(self):
        return self.__status

    def get_group_id(self):
        return self.__group_id

    def __str__(self):
        return "id : %s, name : %s" % (self.__id, self.__name)

    def __unicode__(self):
        return u"id : %s, name : %s" % (self.__id, self.__name)


class GroupComponent:
    def __init__(self, id=None, name=None):
        self.__id = id
        self.__name = name
        pass

    def get_id(self):
        return self.__id

    def get_name(self):
        return self.__name

    def __str__(self):
        return "id : %s, name : %s" % (self.__id, self.__name)

    def __unicode__(self):
        return u"id : %s, name : %s" % (self.__id, self.__name)


class Incident:
    def __init__(self, id=None, name=None, message=None, status=None, visible=None, component_id=None,
                 component_status=None):
        self.__id = id
        self.__name = name
        self.__message = message
        self.__status = status
        self.__visible = visible
        self.__component_id = component_id
        self.__component_status = component_status

    def get_id(self):
        return self.__id

    def get_name(self):
        return self.__name

    def get_message(self):
        return self.__message

    def get_status(self):
        return self.__status

    def get_visible(self):
        return self.__visible

    def get_component_id(self):
        return self.__component_id

    def get_component_status(self):
        return self.__component_status

    def __str__(self):
        return "id : %s" % self.__id

    def __unicode__(self):
        return u"id : %s" % self.__id


class Metric:
    def __init__(self, id, name=None, suffix=None, description=None, default_value=None):
        self.__id = id
        self.__name = name
        self.__suffix = suffix
        self.__description = description
        self.__default_value = default_value
        pass

    def get_id(self):
        return self.__id

    def get_name(self):
        return self.__name

    def get_suffix(self):
        return self.__suffix

    def get_description(self):
        return self.__description

    def get_default_value(self):
        return self.__default_value

    def __str__(self):
        return "id : %s" % self.__id

    def __unicode__(self):
        return u"id : %s" % self.__id


class MetricPoint:
    def __init__(self, metric_id=None, value=None, timestamp=None):
        self.__metric_id = metric_id
        self.__value = value
        self.__timestamp = timestamp
        pass

    def get_metric_id(self):
        return self.__metric_id

    def get_value(self):
        return self.__value

    def get_timestamp(self):
        return self.__timestamp

    def __str__(self):
        return "id : %s" % self.__id

    def __unicode__(self):
        return u"id : %s" % self.__id
