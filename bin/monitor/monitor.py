import ConfigParser
import logging
import psycopg2
import time
from datetime import datetime, timedelta
from dateutil import rrule
from pexpect import pxssh
from time import strftime

from bin.plugins.cachet.cachetplugin import CachetPlugin
from bin.util.constants import ApplicationConstants


class Monitor:

    def __init__(self):
        self.__config = ConfigParser.ConfigParser()
        self.__config.read(ApplicationConstants.DEFAULT_CONFIG_FILE_PATH)
        self.__scheduler = Scheduler.__init__(self.__config)
        self.__crawler = Crawler.__init__(self.__config)
        self.__fetcher = Fetcher.__init__(self.__config)
        self.__database = Database.__init__(self.__config)
        self.__status_type = self.config_section_map("SectionThree")['status_implementation']
        self.__status_implementation = CachetPlugin.__init__()

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

    def check_components(self):
        try:
            self.update_component_status(ApplicationConstants.SCHEDULER_COMPONENT)
            self.update_component_status(ApplicationConstants.CRAWLER_COMPONENT)
            self.update_component_status(ApplicationConstants.FETCHER_COMPONENT)
        except ValueError as e:
            logging.error("Error while updating component statuses", e)

    def update_component_status(self, component):
        if component == ApplicationConstants.SCHEDULER_COMPONENT:
            self.set_scheduler_status()
        elif component == ApplicationConstants.CRAWLER_COMPONENT:
            self.set_crawler_status()
        elif component == ApplicationConstants.FETCHER_COMPONENT:
            self.set_fetcher_status()
        else:
            raise ValueError('Component ' + component + ' does exist!')

    def set_scheduler_status(self):
        is_active = self.get_scheduler_status()
        if is_active == 0:
            self.__status_implementation.update_component_status(ApplicationConstants.SCHEDULER_COMPONENT, 1)
        else:
            self.__status_implementation.set_operation_failure(ApplicationConstants.SCHEDULER_COMPONENT,
                                                               ApplicationConstants.SCHEDULER_COMPONENT + " is down!")

    def get_scheduler_status(self):
        options = {"StrictHostKeyChecking": "yes", "UserKnownHostsFile": "/dev/null"}
        ssh_connection = pxssh.pxssh(options)
        ssh_connection.login(self.__scheduler.get_scheduler_ip(), self.__scheduler.get_scheduler_username())
        scheduler_status = ssh_connection.sendline('if pgrep -x "java" > /dev/null; then; exit 0; else; exit 1; fi')
        # Check if spaces are correct
        return scheduler_status

    def set_crawler_status(self):
        is_active = self.get_crawler_status()
        if is_active == 0:
            return None # only here for now to fix identation
            # TODO: call PUT call to cachet with status 'operational'
        else:
            self.__status_implementation.set_operation_failure(ApplicationConstants.CRAWLER_COMPONENT,
                                                               ApplicationConstants.CRAWLER_COMPONENT + " is down!")

    def get_crawler_status(self):
        options = {"StrictHostKeyChecking": "yes", "UserKnownHostsFile": "/dev/null"}
        ssh_connection = pxssh.pxssh(options)
        ssh_connection.login(self.__crawler.get_crawler_ip(), self.__crawler.get_crawler_username())
        crawler_status = ssh_connection.sendline('if pgrep -x "java" > /dev/null; then; exit 0; else; exit 1; fi')
        # Check if spaces are correct
        return crawler_status

    def set_fetcher_status(self):
        is_active = self.get_fetcher_status()
        if is_active == 0:
            return None # only here for now to fix identation
            # TODO: insert PUT call to cachet with status 'operational'
        else:
            self.__status_implementation.set_operation_failure(ApplicationConstants.FETCHER_COMPONENT,
                                                               ApplicationConstants.FETCHER_COMPONENT + " is down!")

    def get_fetcher_status(self):
        options = {"StrictHostKeyChecking": "yes", "UserKnownHostsFile": "/dev/null"}
        ssh_connection = pxssh.pxssh(options)
        ssh_connection.login(self.__fetcher.get_fetcher_ip(), self.__fetcher.get_fetcher_username())
        fetcher_status = ssh_connection.sendline('if pgrep -x "java" > /dev/null; then; exit 0; else; exit 1; fi')
        # Check if spaces are correct
        return fetcher_status

    def images_status_control(self):
        date_least_one_hour = datetime.today() - timedelta(hours=1)
        date = strftime("%Y-%m-%d %H", date_least_one_hour)
        self.get_processed_images(date)
        self.get_downloaded_images(date)
        self.get_submitted_images(date)
        self.set_last_hour_timestamps(date, ApplicationConstants.DEFAULT_PROCESSED_STATE)
        self.check_last_hours_efficiency()

    def get_processed_images(self, date):
        number_of_processed_images = self.get_number_of_images_with_state_in_last_hour(date, ApplicationConstants.
                                                                                       DEFAULT_PROCESSED_STATE)
        # Number of processed images equal to 0 after three hours it's an operation failure. But we need to know if
        # these three hours already passed to register a failure.
        self.__status_implementation.update_image_number_cachet(number_of_processed_images, ApplicationConstants.
                                                                DEFAULT_PROCESSED_STATE, time.time())

    def get_downloaded_images(self, date):
        number_of_downloaded_images = self.get_number_of_images_with_state_in_last_hour(date, ApplicationConstants.
                                                                                        DEFAULT_DOWNLOADED_STATE)
        self.__status_implementation.update_image_number_cachet(number_of_downloaded_images, ApplicationConstants.
                                                                DEFAULT_DOWNLOADED_STATE, time.time())

    def get_submitted_images(self, date):
        number_of_submitted_images = self.get_number_of_images_with_state_in_last_hour(date, "not_downloaded")
        number_of_submitted_images += self.get_number_of_images_with_state_in_last_hour(date, "selected")
        number_of_submitted_images += self.get_number_of_images_with_state_in_last_hour(date, "downloading")
        self.__status_implementation.update_image_number_cachet(number_of_submitted_images, ApplicationConstants.
                                                                DEFAULT_SUBMITTED_STATE, time.time())

    def get_number_of_images_with_state_in_last_hour(self, date_prefix, state):
        try:
            connection = psycopg2.connect(self.__database.get_db_name(), self.__database.get_db_user(),
                                          self.__database.get_db_password(), self.__database.get_db_host(),
                                          self.__database.get_db_port(), sslmode='verify-full')
            cursor = connection.cursor()

            # Date prefix must follow an established format
            # ex.: 2017-04-12 18 (date previous_hour)
            statement_sql = "SELECT * FROM " + self.__database.get_db_images_table_name() + \
                            " WHERE state = '" + state + "' AND utime::text LIKE '" + date_prefix + "%';"
            cursor.execute(statement_sql)
            return cursor.rowcount
        except psycopg2.Error as e:
            logging.error("Error while getting images in " + state + " state from database", e)
            return e.pgcode

    def check_last_hours_efficiency(self):
        last_hours_date_time = datetime.now() - timedelta(hours=3)
        now = datetime.now()

        processed_images = 0
        for date in rrule.rrule(rrule.HOURLY, dtstart=last_hours_date_time, until=now):
            formatted_date = strftime("%Y-%m-%d %H", date)
            processed_images += self.get_number_of_images_with_state_in_last_hour(formatted_date,
                                                                                  ApplicationConstants.
                                                                                  DEFAULT_PROCESSED_STATE)

    def set_last_hour_timestamps(self, date_prefix, state):
        try:
            connection = psycopg2.connect(self.__database.get_db_name(), self.__database.get_db_user(),
                                          self.__database.get_db_password(), self.__database.get_db_host(),
                                          self.__database.get_db_port(), sslmode='verify-full')
            cursor = connection.cursor()

            # Date prefix must follow an established format
            # ex.: 2017-04-12 18 (date previous_hour)
            statement_sql = "SELECT utime FROM " + self.__database.get_db_images_table_name() + \
                            " WHERE state = '" + state + "' AND utime::text LIKE '" + date_prefix + "%';"
            cursor.execute(statement_sql)
            for record in cursor:
                return None  # only here for now to fix identation
                # TODO: insert POST call to cachet sending image execution time
        except psycopg2.Error as e:
            logging.error("Error while getting images in " + state + " state from database", e)
            return e.pgcode

    def get_crawler_disk_usage(self):
        options = {"StrictHostKeyChecking": "yes", "UserKnownHostsFile": "/dev/null"}
        ssh_connection = pxssh.pxssh(options)
        ssh_connection.login(self.__crawler.get_crawler_ip(), self.__crawler.get_crawler_username())
        disk_usage = ssh_connection.sendline("df -P | awk 'NR==2 {print $5}'").rsplit('%', 1)[0]
        if disk_usage >= 100:
            self.__status_implementation.set_operation_failure(ApplicationConstants.CRAWLER_COMPONENT,
                                                               ApplicationConstants.CRAWLER_COMPONENT
                                                               + " disk is overloaded!")
        return disk_usage

    def get_swift_disk_usage(self):
        # TODO: implement
        # There are some problems here. First thing is that we need to know swift's total disk to determinate a usage
        # percentage. Second thing is that we need a authorization token to communicate with swift, and it must be
        # generated per hour.
        return None


class Scheduler(object):

    def __init__(self, config):
        self.__config = config
        self.__scheduler_ip = self.config_section_map("SectionOne")['scheduler_ip']
        self.__scheduler_port = self.config_section_map("SectionOne")['scheduler_port']
        self.__scheduler_username = self.config_section_map("SectionOne")['scheduler_username']

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

    def get_scheduler_ip(self):
        return self.__scheduler_ip

    def get_scheduler_port(self):
        return self.__scheduler_port

    def get_scheduler_username(self):
        return self.__scheduler_username


class Crawler(object):

    def __init__(self, config):
        self.__config = config
        self.__crawler_ip = self.config_section_map("SectionOne")['crawler_ip']
        self.__crawler_port = self.config_section_map("SectionOne")['crawler_port']
        self.__crawler_username = self.config_section_map("SectionOne")['crawler_username']

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

    def get_crawler_ip(self):
        return self.__crawler_ip

    def get_crawler_port(self):
        return self.__crawler_port

    def get_crawler_username(self):
        return self.__crawler_username


class Fetcher(object):

    def __init__(self, config):
        self.__config = config
        self.__fetcher_ip = self.config_section_map("SectionOne")['fetcher_ip']
        self.__fetcher_port = self.config_section_map("SectionOne")['fetcher_port']
        self.__fetcher_username = self.config_section_map("SectionOne")['fetcher_username']

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

    def get_fetcher_ip(self):
        return self.__fetcher_ip

    def get_fetcher_port(self):
        return self.__fetcher_port

    def get_fetcher_username(self):
        return self.__fetcher_username


class Database(object):

    def __init__(self, config):
        self.__config = config
        self.__db_name = self.config_section_map("SectionTwo")['db_name']
        self.__db_user = self.config_section_map("SectionTwo")['db_user']
        self.__db_password = self.config_section_map("SectionTwo")['db_password']
        self.__db_host = self.config_section_map("SectionTwo")['db_host']
        self.__db_port = self.config_section_map("SectionTwo")['db_port']
        self.__db_images_table_name = self.config_section_map("SectionTwo")['db_images_table_name']

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

    def get_db_name(self):
        return self.__db_name

    def get_db_user(self):
        return self.__db_user

    def get_db_password(self):
        return self.__db_password

    def get_db_host(self):
        return self.__db_host

    def get_db_port(self):
        return self.__db_port

    def get_db_images_table_name(self):
        return self.__db_images_table_name
