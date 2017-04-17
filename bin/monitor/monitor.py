import logging
import ConfigParser
import psycopg2
from time import strftime
from pexpect import pxssh
from dateutil import rrule
from datetime import datetime, timedelta
from bin.util.constants import ApplicationConstants


class Monitor:

    def __init__(self):
        self.__config = ConfigParser.ConfigParser()
        self.__config.read(ApplicationConstants.DEFAULT_CONFIG_FILE_PATH)
        self.__scheduler_ip = self.config_section_map("SectionOne")['scheduler_ip']
        self.__scheduler_port = self.config_section_map("SectionOne")['scheduler_port']
        self.__scheduler_username = self.config_section_map("SectionOne")['scheduler_username']
        self.__crawler_ip = self.config_section_map("SectionOne")['crawler_ip']
        self.__crawler_port = self.config_section_map("SectionOne")['crawler_port']
        self.__crawler_username = self.config_section_map("SectionOne")['crawler_username']
        self.__fetcher_ip = self.config_section_map("SectionOne")['fetcher_ip']
        self.__fetcher_port = self.config_section_map("SectionOne")['fetcher_port']
        self.__fetcher_username = self.config_section_map("SectionOne")['fetcher_username']
        self.__db_name = self.config_section_map("SectionTwo")['db_name']
        self.__db_user = self.config_section_map("SectionTwo")['db_user']
        self.__db_password = self.config_section_map("SectionTwo")['db_password']
        self.__db_host = self.config_section_map("SectionTwo")['db_host']
        self.__db_port = self.config_section_map("SectionTwo")['db_port']
        self.__db_images_table_name = self.config_section_map("SectionTwo")['db_images_table_name']
        self.__status_implementation = self.config_section_map("SectionThree")['status_implementation']
        self.__cachet_host_url = self.config_section_map("SectionThree")['cachet_host_url']

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
            return None  # only here for now to fix identation
            # TODO: call POST call to cachet with status 'operational'
        else:
            return None  # only here for now to fix identation
            # TODO: call POST call to cachet with status 'major outage'

    def get_scheduler_status(self):
        options = {"StrictHostKeyChecking": "yes", "UserKnownHostsFile": "/dev/null"}
        ssh_connection = pxssh.pxssh(options)
        ssh_connection.login(self.__scheduler_ip, self.__scheduler_username)
        scheduler_status = ssh_connection.sendline('if pgrep -x "java" > /dev/null; then; exit 0; else; exit 1; fi')
        # Check if spaces are correct
        if scheduler_status != 0:
            self.set_operation_failure()
        return scheduler_status

    def set_crawler_status(self):
        is_active = self.get_crawler_status()
        if is_active == 0:
            return None # only here for now to fix identation
            # TODO: call POST call to cachet with status 'operational'
        else:
            return None  # only here for now to fix identation
            # TODO: call POST call to cachet with status 'major outage'

    def get_crawler_status(self):
        options = {"StrictHostKeyChecking": "yes", "UserKnownHostsFile": "/dev/null"}
        ssh_connection = pxssh.pxssh(options)
        ssh_connection.login(self.__crawler_ip, self.__crawler_username)
        crawler_status = ssh_connection.sendline('if pgrep -x "java" > /dev/null; then; exit 0; else; exit 1; fi')
        # Check if spaces are correct
        if crawler_status != 0:
            self.set_operation_failure()
        return crawler_status

    def set_fetcher_status(self):
        is_active = self.get_fetcher_status()
        if is_active == 0:
            return None # only here for now to fix identation
            # TODO: insert POST call to cachet with status 'operational'
        else:
            return None  # only here for now to fix identation
            # TODO: insert POST call to cachet with status 'major outage'

    def get_fetcher_status(self):
        options = {"StrictHostKeyChecking": "yes", "UserKnownHostsFile": "/dev/null"}
        ssh_connection = pxssh.pxssh(options)
        ssh_connection.login(self.__fetcher_ip, self.__fetcher_username)
        fetcher_status = ssh_connection.sendline('if pgrep -x "java" > /dev/null; then; exit 0; else; exit 1; fi')
        # Check if spaces are correct
        if fetcher_status != 0:
            self.set_operation_failure()
        return fetcher_status

    def images_status_control(self):
        date_least_one_hour = datetime.today() - timedelta(hours=1)
        date = strftime("%Y-%m-%d %H", date_least_one_hour)
        self.get_processed_images(date)
        self.get_downloaded_images(date)
        self.get_submitted_images(date)
        self.set_images_times(date)
        self.check_images_in_number_of_hours()

    def get_processed_images(self, date):
        number_of_processed_images = self.get_number_of_images_with_state_in_last_hour(date, ApplicationConstants.
                                                                                       DEFAULT_PROCESSED_STATE)
        # Number of processed images equal to 0 after three hours it's an operation failure. But we need to know if
        # these three hours already passed to register a failure.
        self.handle_images(number_of_processed_images, ApplicationConstants.DEFAULT_PROCESSED_STATE)

    def get_downloaded_images(self, date):
        number_of_downloaded_images = self.get_number_of_images_with_state_in_last_hour(date, ApplicationConstants.
                                                                                        DEFAULT_DOWNLOADED_STATE)
        self.handle_images(number_of_downloaded_images, ApplicationConstants.DEFAULT_DOWNLOADED_STATE)

    def get_submitted_images(self, date):
        number_of_submitted_images = self.get_number_of_images_with_state_in_last_hour(date, "not_downloaded")
        number_of_submitted_images += self.get_number_of_images_with_state_in_last_hour(date, "selected")
        number_of_submitted_images += self.get_number_of_images_with_state_in_last_hour(date, "downloading")
        self.handle_images(number_of_submitted_images, ApplicationConstants.DEFAULT_SUBMITTED_STATE)

    def get_number_of_images_with_state_in_last_hour(self, date_prefix, state):
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
        if self.__status_implementation == ApplicationConstants.DEFAULT_STATUS_IMPLEMENTATION:
            self.update_image_number_cachet(number_of_images, state)

    def check_images_in_number_of_hours(self):
        last_hours_date_time = datetime.now() - timedelta(hours=3)
        now = datetime.now()

        processed_images = 0
        for date in rrule.rrule(rrule.HOURLY, dtstart=last_hours_date_time, until=now):
            formatted_date = strftime("%Y-%m-%d %H", date)
            processed_images += self.get_number_of_images_with_state_in_last_hour(formatted_date,
                                                                                  ApplicationConstants.
                                                                                  DEFAULT_PROCESSED_STATE)

        if processed_images == 0:
            self.set_operation_failure()

    def set_images_times(self, date):
        self.set_images_times_with_state_in_last_hour(date, ApplicationConstants.DEFAULT_PROCESSED_STATE)

    def set_images_times_with_state_in_last_hour(self, date_prefix, state):
        try:
            connection = psycopg2.connect(self.__db_name, self.__db_user, self.__db_password, self.__db_host,
                                          self.__db_port, sslmode='verify-full')
            cursor = connection.cursor()

            # Date prefix must follow an established format
            # ex.: 2017-04-12 18 (date previous_hour)
            statement_sql = "SELECT utime FROM " + self.__db_images_table_name + \
                            " WHERE state = '" + state + "' AND utime::text LIKE '" + date_prefix + "%';"
            cursor.execute(statement_sql)
            for record in cursor:
                return None  # only here for now to fix identation
                # TODO: insert POST call to cachet sending image execution time
        except psycopg2.Error as e:
            logging.error("Error while getting images in " + state + " state from database", e)
            return e.pgcode

    def update_image_number_cachet(self, number_of_images, state):
        # TODO: call cachet POST
        return None

    def get_crawler_disk_usage(self):
        options = {"StrictHostKeyChecking": "yes", "UserKnownHostsFile": "/dev/null"}
        ssh_connection = pxssh.pxssh(options)
        ssh_connection.login(self.__crawler_ip, self.__crawler_username)
        return ssh_connection.sendline("df -P | awk 'NR==2 {print $5}'")  # String returned here will be in format
        # value%

    def get_swift_disk_usage(self):
        # TODO: implement
        # There are some problems here. First thing is that we need to know swift's total disk to determinate a usage
        # percentage. Second thing is that we need a authorization token to communicate with swift, and it must be
        # generated per hour.
        return None

    def set_operation_failure(self):
        # TODO: implement
        # Here, we will register an operation failure and an incident. So it might change to receive a message based on
        # the failure for we to know, automatically, which failure caused the incident.
        return None

    def set_incident(self):
        # TODO: implement
        return None
