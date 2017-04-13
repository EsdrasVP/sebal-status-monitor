import logging
import ConfigParser
import psycopg2
from time import strftime
from datetime import datetime, timedelta


class Monitor:

    DEFAULT_STATUS_IMPLEMENTATION = "cachet"
    DEFAULT_CONFIG_FILE_PATH = "resources/config.ini"  # TODO: see if this path is correct
    DEFAULT_PROCESSED_STATE = "fetched"
    DEFAULT_DOWNLOADED_STATE = "downloaded"
    DEFAULT_SUBMITTED_STATE = "submitted"

    def __init__(self):
        self.__config = ConfigParser.ConfigParser()
        self.__config.read(self.DEFAULT_CONFIG_FILE_PATH)
        self.__db_name = self.config_section_map("SectionOne")['db_name']
        self.__db_user = self.config_section_map("SectionOne")['db_user']
        self.__db_password = self.config_section_map("SectionOne")['db_password']
        self.__db_host = self.config_section_map("SectionOne")['db_host']
        self.__db_port = self.config_section_map("SectionOne")['db_port']
        self.__db_images_table_name = self.config_section_map("SectionOne")['db_images_table_name']
        self.__status_implementation = self.config_section_map("SectionOne")['status_implementation']
        self.__cachet_host_url = self.config_section_map("SectionOne")['cachet_host_url']

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

    def start(self):
        self.check_components()
        self.images_status_control()
        self.get_crawler_disk_usage()
        self.get_swift_disk_usage()
        self.get_operation_failures()

    def check_components(self):
        self.update_component_status("scheduler")
        self.update_component_status("crawler")
        self.update_component_status("fetcher")

    def update_component_status(self, component):
        # TODO: call cachet POST
        return None

    def images_status_control(self):
        date_least_one_hour = datetime.today() - timedelta(hours=1)
        date = strftime("%Y-%m-%d %H", date_least_one_hour)
        self.get_processed_images(date)
        self.get_downloaded_images(date)
        self.get_submitted_images(date)
        self.get_images_execution_time(date)

    def get_processed_images(self, date):
        number_of_processed_images = self.get_images_in_state_last_hour(date, self.DEFAULT_PROCESSED_STATE)
        self.handle_images(number_of_processed_images, self.DEFAULT_PROCESSED_STATE)

    def get_downloaded_images(self, date):
        number_of_downloaded_images = self.get_images_in_state_last_hour(date, self.DEFAULT_DOWNLOADED_STATE)
        self.handle_images(number_of_downloaded_images, self.DEFAULT_DOWNLOADED_STATE)

    def get_submitted_images(self, date):
        number_of_submitted_images = self.get_images_in_state_last_hour(date, "not_downloaded")
        number_of_submitted_images += self.get_images_in_state_last_hour(date, "selected")
        number_of_submitted_images += self.get_images_in_state_last_hour(date, "downloading")
        self.handle_images(number_of_submitted_images, self.DEFAULT_SUBMITTED_STATE)

    def get_images_execution_time(self, date):
        # TODO: implement
        return None

    def get_images_in_state_last_hour(self, date_prefix, state):
        try:
            connection = psycopg2.connect(self.__db_name, self.__db_user, self.__db_password, self.__db_host,
                                          self.__db_port, sslmode='verify-full')
            cursor = connection.cursor()

            # Date prefix must follow an established format
            # ex.: 2017-04-12 18 (date previous_hour)
            statement_sql = "SELECT * FROM " + self.__db_images_table_name + \
                            " WHERE state = '" + state + "' AND utime::text LIKE '" + date_prefix + "%';"
            cursor.execute(statement_sql)
            return cursor.rowcount
        except psycopg2.Error as e:
            logging.error("Error while getting images in " + state + " state from database", e)
            return e.pgcode

    def handle_images(self, number_of_images, state):
        if self.__status_implementation == self.DEFAULT_STATUS_IMPLEMENTATION:
            self.update_image_number_cachet(number_of_images, state)

    def update_image_number_cachet(self, number_of_images, state):
        # TODO: call cachet POST
        return None

    def get_crawler_disk_usage(self):
        # TODO: implement
        return None

    def get_swift_disk_usage(self):
        # TODO: implement
        return None

    def get_operation_failures(self):
        # TODO: implement
        return None
