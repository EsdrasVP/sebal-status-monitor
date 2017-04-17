import ConfigParser
from bin.util.constants import ApplicationConstants

class CachetHelper:

    def __init__(self):
        self.__config = ConfigParser.ConfigParser()
        self.__config.read(ApplicationConstants.DEFAULT_CONFIG_FILE_PATH)
        self.__cachet_host_url = self.config_section_map("SectionThree")['cachet_host_url']
        self.__status_implementation = self.config_section_map("SectionThree")['status_implementation']

    def update_image_number_cachet(self, number_of_images, state):
        # TODO: call cachet POST
        return None

    def set_incident(self):
        # TODO: implement
        return None

    def set_operation_failure(self):
        # TODO: implement
        # Here, we will register an operation failure and an incident. So it might change to receive a message based on
        # the failure for we to know, automatically, which failure caused the incident.
        return None