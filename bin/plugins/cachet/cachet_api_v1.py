import urllib
import urllib2
import logging
import os

CONST_SUFIX_INTENT_URL = "/api/v1/incidents"
CONST_SUFIX_COMPONENTS_URL = "/api/v1/components"
CONST_SUFIX_GROUP_COMPONENTS_URL = "/api/v1/components/groups"
CONST_SUFIX_METRIC_URL = "/api/v1/metrics"
CONST_SUFIX_METRIC_POINT_URL = "/points"
CONST_HEADER_APP_KEY = "X-Cachet-Token"

CONST_DELETE_METHOD = "DELETE"
CONST_POST_METHOD = "POST"
CONST_PUT_METHOD = "PUT"

CONST_NAME_ATTR = "name"
CONST_MESSAGE_ATTR = "message"
CONST_STATUS_ATTR = "status"
CONST_GROUP_ID_ATTR = "group_id"
CONST_VISIBLE_ATTR = "visible"
CONST_COMPONENT_ID_ATTR = "component_id"
CONST_COMPONENT_STATUS_ATTR = "component_status"
CONST_SUFFIX_ATTR = "suffix"
CONST_DESCRIPTION_ATTR = "description"
CONST_DEFAULT_VALUE_ATTR = "default_value"
CONST_VALUE_ATTR = "value"
CONST_TIMESTAMP_ATTR = "timestamp"

CONST_OPERATIONAL_STATUS = 1

logger = logging.getLogger(__name__)


# Cachet
# Api v1
# Ref : https://docs.cachethq.io/reference#post-metric-points
class CachetApiV1:

    def __init__(self):
        '''__init__ Constructor'''
        pass

    @staticmethod
    def get_groups_component(endpoint):
        logger.debug("Getting groups component in %s." % (endpoint))
        try:
            url = "%s%s" % (endpoint, CONST_SUFIX_GROUP_COMPONENTS_URL)
            return urllib2.urlopen(url).read()
        except Exception as e:
            logging.exception("Error while get groups component.")
            return None

    @staticmethod
    def get_group_component(endpoint, id):
        logger.debug("Getting group component(%s) in %s." % (id, endpoint))
        try:
            url = "%s%s/%s" % (endpoint, CONST_SUFIX_GROUP_COMPONENTS_URL, id)
            return urllib2.urlopen(url).read()
        except Exception as e:
            logging.exception("Error while get group component(%s)." % (id))
            return None

    # TODO implement
    @staticmethod
    def create_group_component(endpoint, name, token):
        logger.debug("Creating group component in %s." % (endpoint))
        try:
            method = CONST_POST_METHOD
            url = "%s%s" % (endpoint, CONST_SUFIX_GROUP_COMPONENTS_URL)

            data = urllib.urlencode({CONST_NAME_ATTR: name})

            headers = {CONST_HEADER_APP_KEY: token}

            request = urllib2.Request(url, data=data, headers=headers)
            request.get_method = lambda: method
            response = urllib2.urlopen(request)
            return response.read()
        except Exception as e:
            logging.exception("Error while create group component.")
            return None

    # TODO implement
    @staticmethod
    def delete_group_component(endpoint, id, token):
        logger.debug("Deleting group component(%s) in %s." % (id, endpoint))
        try:
            method = CONST_DELETE_METHOD
            url = "%s%s/%s" % (endpoint, CONST_SUFIX_GROUP_COMPONENTS_URL, id)

            headers = {CONST_HEADER_APP_KEY: token}

            request = urllib2.Request(url, data=None, headers=headers)
            request.get_method = lambda: method
            response = urllib2.urlopen(request)
            return response.read()
        except Exception as e:
            logging.exception("Error while delete group component(%s)." % (id))
            return None

    @staticmethod
    def get_components(endpoint):
        logger.debug("Getting components in %s." % (endpoint))
        try:
            url = "%s%s" % (endpoint, CONST_SUFIX_COMPONENTS_URL)
            return urllib2.urlopen(url).read()
        except Exception as e:
            logging.exception("Error while get components.")
            return None

    @staticmethod
    def get_component(endpoint, id):
        logger.debug("Getting component(%s) in %s." % (id, endpoint))
        try:
            url = "%s%s/%s" % (endpoint, CONST_SUFIX_COMPONENTS_URL, id)
            return urllib2.urlopen(url).read()
        except Exception as e:
            logging.exception("Error while get component(%s)." % (id))
            return None

    @staticmethod
    def delete_component(endpoint, id, token):
        logger.debug("Deleting component(%s) in %s." % (id, endpoint))
        try:
            method = CONST_DELETE_METHOD
            url = "%s%s/%s" % (endpoint, CONST_SUFIX_COMPONENTS_URL, id)

            headers = {CONST_HEADER_APP_KEY: token}

            request = urllib2.Request(url, data=None, headers=headers)
            request.get_method = lambda: method
            response = urllib2.urlopen(request)
            return response.read()
        except Exception as e:
            logging.exception("Error while delete component(%s)." % (id))
            return None

    # TODO implement more tests
    @staticmethod
    def create_component(endpoint, name, status, group_id, token):
        logger.debug("Creating component in %s." % (endpoint))
        try:
            method = CONST_POST_METHOD
            url = "%s%s" % (endpoint, CONST_SUFIX_COMPONENTS_URL)

            datas = {CONST_NAME_ATTR: name, CONST_STATUS_ATTR: status}
            if status is None:
                datas.update({CONST_STATUS_ATTR: CONST_OPERATIONAL_STATUS})
            if group_id is not None:
                datas.update({CONST_GROUP_ID_ATTR: group_id})
            data = urllib.urlencode(datas)

            headers = {CONST_HEADER_APP_KEY: token}

            request = urllib2.Request(url, data=data, headers=headers)
            request.get_method = lambda: method
            response = urllib2.urlopen(request)
            return response.read()
        except Exception as e:
            logging.exception("Error while create component.")
            return None

            # TODO implement

    @staticmethod
    def update_component_status(endpoint, name, status, group_id, token):
        logger.debug("Updating component " + name + " status in %s." % (endpoint))
        try:
            method = CONST_PUT_METHOD
            url = "%s%s" % (endpoint, CONST_SUFIX_COMPONENTS_URL)

            datas = {CONST_NAME_ATTR: name, CONST_STATUS_ATTR: status}
            if status is None:
                datas.update({CONST_STATUS_ATTR: CONST_OPERATIONAL_STATUS})
            data = urllib.urlencode(datas)

            headers = {CONST_HEADER_APP_KEY: token}

            request = urllib2.Request(os.path.join(url, group_id), data=data, headers=headers)
            request.get_method = lambda: method
            response = urllib2.urlopen(request)
            return response.read()
        except Exception as e:
            logging.exception("Error while update component.")
            return None

    @staticmethod
    def get_intents(endpoint):
        pass

        # TODO implement

    @staticmethod
    def get_intent(endpoint):
        pass

        # TODO implement

    @staticmethod
    def delete_intent(endpoint):
        pass

    # TODO refactor
    @staticmethod
    def create_intent(endpoint, name, message, status, visible, componentId, componentStatus, token):
        logger.debug("Creating intent in %s." % (endpoint))
        try:
            method = CONST_POST_METHOD
            url = "%s%s" % (endpoint, CONST_SUFIX_INTENT_URL)

            datas = {CONST_NAME_ATTR: name, CONST_MESSAGE_ATTR: message,
                     CONST_STATUS_ATTR: status, CONST_VISIBLE_ATTR: visible}
            if componentId is not None and componentStatus is not None:
                datas.update({CONST_COMPONENT_ID_ATTR: componentId, CONST_COMPONENT_STATUS_ATTR: componentStatus})
            data = urllib.urlencode(datas)

            headers = {CONST_HEADER_APP_KEY: token}

            request = urllib2.Request(url, data=data, headers=headers)
            request.get_method = lambda: method
            response = urllib2.urlopen(request)
            return response.read()
        except Exception as e:
            logging.exception("Error while create intent.")
            return None

    # TODO implement
    @staticmethod
    def get_metrics(endpoint):
        logger.debug("Getting metrics in %s." % (endpoint))
        try:
            url = "%s%s" % (endpoint, CONST_SUFIX_METRIC_URL)
            return urllib2.urlopen(url).read()
        except Exception as e:
            logging.exception("Error while get metrics.")
            return None

    # TODO implement
    @staticmethod
    def get_metric(endpoint):
        pass

    # TODO implement
    @staticmethod
    def delete_metric(endpoint, token):
        pass

    # TODO refactor
    # name, suffix, descripion : string
    # default_value : int
    @staticmethod
    def create_metric(endpoint, name, suffix, description, default_value, token):
        logger.debug("Creating metric in %s." % (endpoint))
        try:
            method = CONST_POST_METHOD
            url = "%s%s" % (endpoint, CONST_SUFIX_METRIC_URL)

            datas = {CONST_NAME_ATTR: name, CONST_SUFFIX_ATTR: suffix,
                     CONST_DESCRIPTION_ATTR: description, CONST_DEFAULT_VALUE_ATTR: default_value}
            data = urllib.urlencode(datas)

            headers = {CONST_HEADER_APP_KEY: token}

            request = urllib2.Request(url, data=data, headers=headers)
            request.get_method = lambda: method
            response = urllib2.urlopen(request)
            return response.read()
        except Exception as e:
            logging.exception("Error while create metric.")
            return None

    # TODO refactor
    # TODO test with timestamp
    # metric_ic : int
    # value_point : double
    # timestamp : string
    @staticmethod
    def create_metric_point(endpoint, metric_id, value_point, timestamp, token):
        logger.debug("Creating metric point in %s." % (endpoint))
        try:
            method = CONST_POST_METHOD
            url = "%s%s/%s%s" % (endpoint, CONST_SUFIX_METRIC_URL, metric_id, CONST_SUFIX_METRIC_POINT_URL)

            datas = {CONST_VALUE_ATTR: value_point}
            if timestamp is not None:
                datas.update({CONST_TIMESTAMP_ATTR: timestamp})
            data = urllib.urlencode(datas)

            headers = {CONST_HEADER_APP_KEY: token}

            request = urllib2.Request(url, data=data, headers=headers)
            request.get_method = lambda: method
            response = urllib2.urlopen(request)
            return response.read()
        except Exception as e:
            logging.exception("Error while create metric point.")
            return None
