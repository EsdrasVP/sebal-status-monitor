import logging
import ConfigParser
import psycopg2
import time
from time import strftime
from datetime import datetime, timedelta


class Monitor:

    DEFAULT_MONITOR_SLEEP_TIME = 3600
    DEFAULT_STATUS_IMPLEMENTATION = "cachet"
    DEFAULT_CONFIG_FILE_PATH = "resources/config.ini"  # TODO: see if this path is correct

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

    def start(self):
        date_least_one_hour = datetime.today() - timedelta(hours=1)
        date = strftime("%Y-%m-%d %H", date_least_one_hour)
        number_of_processed_images = self.get_processed_images_last_hour(date)
        self.handle_processed_images(number_of_processed_images)
        time.sleep(self.DEFAULT_MONITOR_SLEEP_TIME)

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

    def get_processed_images_last_hour(self, date_prefix):
        try:
            connection = psycopg2.connect(self.__db_name, self.__db_user, self.__db_password, self.__db_host,
                                          self.__db_port, sslmode='verify-full')
            cursor = connection.cursor()

            # Date prefix must follow an established format
            # ex.: 2017-04-12 18 (date previous_hour)
            statement_sql = "SELECT * FROM " + self.__db_images_table_name + \
                            " WHERE state = 'fetched' AND utime::text LIKE '" + date_prefix + "%';"
            cursor.execute(statement_sql)
            return cursor.rowcount
        except psycopg2.Error as e:
            logging.error("error while getting number of processed images", e)
            return e.pgcode

    def handle_processed_images(self, number_of_processed_images):
        if self.__status_implementation == self.DEFAULT_STATUS_IMPLEMENTATION:
            self.update_image_number_cachet(number_of_processed_images)

    def update_image_number_cachet(self, number_of_processed_images):
        # TODO: call cachet POST
        return None
