import time
import logging
import psycopg2
import subprocess
import ConfigParser
from time import strftime
from dateutil import rrule
from datetime import datetime, timedelta

from bin.util.constants import ApplicationConstants
from bin.plugins.cachet.cachetplugin import CachetPlugin


class Monitor:

    def __init__(self):
        self.__config = ConfigParser.ConfigParser()
        self.__config.read(ApplicationConstants.DEFAULT_CONFIG_FILE_PATH)
        self.__scheduler = Scheduler(config=self.__config)
        self.__crawlers_list = []
        self.__fetcher = Fetcher(config=self.__config)
        self.__database = Database(config=self.__config)
        self.__status_type = self.config_section_map("SectionThree")['status_implementation']
        self.__private_key_file_path = self.config_section_map("SectionSix")['private_key_file_path']
        if self.__status_type == ApplicationConstants.CACHET_TYPE:
            self.__status_implementation = CachetPlugin()

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
        self.get_all_crawlers()
        self.check_components()
        self.images_status_control()
        self.set_disk_statistics()

    def get_all_crawlers(self):
        try:
            connection = psycopg2.connect(database=self.__database.get_db_name(), user=self.__database.get_db_user(),
                                          password=self.__database.get_db_password(),
                                          host=self.__database.get_db_host(),
                                          port=self.__database.get_db_port())
            cursor = connection.cursor()

            statement_sql = "SELECT * FROM " + self.__database.get_db_deploy_config_table_name() + ";"
            cursor.execute(statement_sql)

            crawler_list = []
            for row in cursor:
                crawler_list.append(Crawler(ip=row[0], port=row[3],
                                            username=ApplicationConstants.DEFAULT_CRAWLER_USERNAME, site=row[2]))

            self.__crawlers_list = crawler_list
        except psycopg2.Error as e:
            logging.error("Error while getting crawlers from database", e)
            return e.pgcode

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
        scheduler_component = self.__status_implementation.get_component_by_name(ApplicationConstants.
                                                                                 SCHEDULER_COMPONENT)
        if (scheduler_component is None) or (not scheduler_component):
            self.__status_implementation.create_component_by_name(ApplicationConstants.SCHEDULER_COMPONENT)

        is_active = self.get_scheduler_status()
        if is_active == 0:
            self.__status_implementation.update_component_status(ApplicationConstants.SCHEDULER_COMPONENT, 1)
        else:
            self.__status_implementation.set_operation_failure(ApplicationConstants.SCHEDULER_COMPONENT,
                                                               ApplicationConstants.SCHEDULER_COMPONENT +
                                                               " is down!")

    def get_scheduler_status(self):
        command = 'ps xau | grep java | grep SebalMain | wc -l'
        process_output = subprocess.check_output(["ssh", "-i", self.__private_key_file_path, "-o",
                                                  "UserKnownHostsFile=/dev/null", "-o", "StrictHostKeyChecking=no",
                                                  self.__scheduler.get_scheduler_username() + "@" +
                                                  self.__scheduler.get_scheduler_ip(), command])
        process_count = int(process_output) - 1
        if process_count >= 1:
            return 0

        return 1

    def set_crawler_status(self):
        for crawler in self.__crawlers_list:
            self.update_crawler_status(crawler.get_crawler_ip(), crawler.get_crawler_username(),
                                       crawler.get_crawler_site())

    def update_crawler_status(self, crawler_ip, crawler_username, crawler_site):
        crawler_component = self.__status_implementation.get_component_by_name(ApplicationConstants.CRAWLER_COMPONENT +
                                                                               crawler_site)
        if (crawler_component is None) or (not crawler_component):
            self.__status_implementation.create_component_by_name(ApplicationConstants.CRAWLER_COMPONENT + crawler_site)

        is_active = self.get_crawler_status(crawler_ip, crawler_username)
        if is_active == 0:
            self.__status_implementation.update_component_status(ApplicationConstants.CRAWLER_COMPONENT +
                                                                 crawler_site, 1)
        else:
            self.__status_implementation.set_operation_failure(ApplicationConstants.CRAWLER_COMPONENT + crawler_site,
                                                               ApplicationConstants.CRAWLER_COMPONENT +
                                                               crawler_site + " is down!")

    def get_crawler_status(self, crawler_ip, crawler_username):
        command = 'ps xau | grep java | grep CrawlerMain | wc -l'
        process_output = subprocess.check_output(["ssh", "-i", self.__private_key_file_path, "-o",
                                                  "UserKnownHostsFile=/dev/null", "-o", "StrictHostKeyChecking=no",
                                                  crawler_username + "@" + crawler_ip, command])
        process_count = int(process_output) - 1
        if process_count >= 1:
            return 0

        return 1

    def set_fetcher_status(self):
        fetcher_component = self.__status_implementation.get_component_by_name(ApplicationConstants.FETCHER_COMPONENT)
        if (fetcher_component is None) or (not fetcher_component):
            self.__status_implementation.create_component_by_name(ApplicationConstants.FETCHER_COMPONENT)

        is_active = self.get_fetcher_status()
        if is_active == 0:
            self.__status_implementation.update_component_status(ApplicationConstants.FETCHER_COMPONENT, 1)
        else:
            self.__status_implementation.set_operation_failure(ApplicationConstants.FETCHER_COMPONENT,
                                                               ApplicationConstants.FETCHER_COMPONENT + " is down!")

    def get_fetcher_status(self):
        command = 'ps xau | grep java | grep FetcherMain | wc -l'
        process_output = subprocess.check_output(["ssh", "-i", self.__private_key_file_path, "-o",
                                                  "UserKnownHostsFile=/dev/null", "-o", "StrictHostKeyChecking=no",
                                                  self.__fetcher.get_fetcher_username() + "@" +
                                                  self.__fetcher.get_fetcher_ip(), command])
        process_count = int(process_output) - 1
        if process_count >= 1:
            return 0

        return 1

    def images_status_control(self):
        date = self.get_last_one_hour_date()
        self.get_processed_images(date)
        self.get_downloaded_images(date)
        self.get_submitted_images(date)
        self.set_last_hour_timestamps(date)
        self.check_last_hours_efficiency()

    @staticmethod
    def get_last_one_hour_date():
        date_least_one_hour = datetime.today() - timedelta(hours=1)
        date = strftime("%Y-%m-%d %H", date_least_one_hour.timetuple())
        return date

    def get_processed_images(self, date):
        number_of_processed_images = self.get_number_of_images_with_state_in_last_hour(date, ApplicationConstants.
                                                                                       DEFAULT_PROCESSED_STATE)
        # Number of processed images equal to 0 after three hours it's an operation failure. But we need to know if
        # these three hours already passed to register a failure.
        self.__status_implementation.update_metric_point(number_of_processed_images, ApplicationConstants.
                                                         PROCESSED_IMAGES_METRIC_NAME, time.time())

    def get_downloaded_images(self, date):
        number_of_downloaded_images = self.get_number_of_images_with_state_in_last_hour(date, ApplicationConstants.
                                                                                        DEFAULT_DOWNLOADED_STATE)
        self.__status_implementation.update_metric_point(number_of_downloaded_images, ApplicationConstants.
                                                         DOWNLOADED_IMAGES_METRIC_NAME, time.time())

    def get_submitted_images(self, date):
        number_of_submitted_images = self.get_number_of_images_with_state_in_last_hour(date, ApplicationConstants.
                                                                                       DEFAULT_NOT_DOWNLOADED_STATE)
        number_of_submitted_images += self.get_number_of_images_with_state_in_last_hour(date, ApplicationConstants.
                                                                                        DEFAULT_SELECTED_STATE)
        number_of_submitted_images += self.get_number_of_images_with_state_in_last_hour(date, ApplicationConstants.
                                                                                        DEFAULT_DOWNLOADING_STATE)
        self.__status_implementation.update_metric_point(number_of_submitted_images, ApplicationConstants.
                                                         SUBMITTED_IMAGES_METRIC_NAME, time.time())

    def get_number_of_images_with_state_in_last_hour(self, date_prefix, state):
        try:
            connection = psycopg2.connect(database=self.__database.get_db_name(), user=self.__database.get_db_user(),
                                          password=self.__database.get_db_password(),
                                          host=self.__database.get_db_host(),
                                          port=self.__database.get_db_port())
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
            formatted_date = strftime("%Y-%m-%d %H", date.timetuple())
            processed_images += self.get_number_of_images_with_state_in_last_hour(formatted_date,
                                                                                  ApplicationConstants.
                                                                                  DEFAULT_PROCESSED_STATE)

    def set_last_hour_timestamps(self, date_prefix):
        try:
            connection = psycopg2.connect(database=self.__database.get_db_name(), user=self.__database.get_db_user(),
                                          password=self.__database.get_db_password(),
                                          host=self.__database.get_db_host(),
                                          port=self.__database.get_db_port())
            cursor = connection.cursor()

            # Date prefix must follow an established format
            # ex.: 2017-04-12 18 (date previous_hour)
            statement_sql = "SELECT utime FROM " + self.__database.get_db_images_table_name() + \
                            " WHERE state = '" + ApplicationConstants.DEFAULT_PROCESSED_STATE + \
                            "' AND utime::text LIKE '" + date_prefix + "%';"
            cursor.execute(statement_sql)
            response = cursor.fetchone()
            date = datetime.strptime('%Y-%m-%d %H:%M:%S.%f', str(response[0].timetuple()))
            epoch = datetime.datetime.utcfromtimestamp(0)
            date_in_millis = (date - epoch).total_seconds() * 1000.0
            self.__status_implementation.update_metric_point(date_in_millis,
                                                             ApplicationConstants.AVG_EXECUTION_TIME_METRIC_NAME,
                                                             time.time())
        except psycopg2.Error as e:
            logging.error("Error while getting images in " + ApplicationConstants.DEFAULT_PROCESSED_STATE +
                          " state from database", e)
            return e.pgcode

    def set_disk_statistics(self):
        self.set_crawler_disk_usage()
        self.set_swift_disk_usage()

    def set_crawler_disk_usage(self):
        for crawler in self.__crawlers_list:
            self.get_crawler_disk_statistic(crawler.get_crawler_ip, crawler.get_crawler_username,
                                            crawler.get_crawler_site)

    def get_crawler_disk_statistic(self, crawler_ip, crawler_username, crawler_site):
        crawler_disk_usage = self.get_crawler_disk_usage(crawler_ip, crawler_username)
        self.__status_implementation.update_metric_point(crawler_disk_usage,
                                                         ApplicationConstants.CRAWLER_DISK_USAGE_METRIC_NAME +
                                                         crawler_site, time.time())

    def set_swift_disk_usage(self):
        swift_disk_usage = self.get_swift_disk_usage()
        self.__status_implementation.update_metric_point(swift_disk_usage,
                                                         ApplicationConstants.SWIFT_DISK_USAGE_METRIC_NAME,
                                                         time.time())

    def get_crawler_disk_usage(self, crawler_ip, crawler_username):
        command = "df -P | awk 'NR==2 {print $5}'"
        process_output = subprocess.check_output(["ssh", "-i", self.__private_key_file_path, "-o",
                                                  "UserKnownHostsFile=/dev/null", "-o", "StrictHostKeyChecking=no",
                                                  crawler_username + "@" + crawler_ip, command])
        disk_usage = process_output.rsplit('%', 1)[0]
        if int(disk_usage) >= 100:
            self.__status_implementation.set_operation_failure(ApplicationConstants.CRAWLER_COMPONENT,
                                                               ApplicationConstants.CRAWLER_COMPONENT
                                                               + " disk is overloaded!")
        return disk_usage

    # see if swift will be a component
    def get_swift_disk_usage(self):
        swift = Swift(config=self.__config)
        response = subprocess.check_output(["swift", "--os-auth-token", swift.get_auth_token(), "--os-storage-url",
                                            swift.get_storage_url(), "stat", swift.get_container_name()])
        response_split = response.split()
        total_used_bytes = response_split[7]
        used_disk_in_gb = float(total_used_bytes)/1073741824
        disk_usage = (100 * used_disk_in_gb) / float(swift.get_total_disk())
        return disk_usage


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

    def __init__(self, ip, port, username, site):
        self.__crawler_ip = ip
        self.__crawler_port = port
        self.__crawler_username = username
        self.__crawler_site = site

    def get_crawler_ip(self):
        return self.__crawler_ip

    def get_crawler_port(self):
        return self.__crawler_port

    def get_crawler_username(self):
        return self.__crawler_username

    def get_crawler_site(self):
        return self.__crawler_site


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
        self.__db_deploy_config_table_name = self.config_section_map("SectionTwo")['db_deploy_config_table_name']

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

    def get_db_deploy_config_table_name(self):
        return self.__db_deploy_config_table_name


class Swift:

    def __init__(self, config):
        self.__config = config
        self.__auth_token = self.generate_auth_token()
        self.__storage_url = self.config_section_map("SectionFive")['swift_storage_url']
        self.__container_name = self.config_section_map("SectionFive")['swift_container_name']
        self.__total_disk = self.config_section_map("SectionFive")['swift_total_disk']

    def generate_auth_token(self):
        fogbow_cli_path = self.config_section_map("SectionFour")['fogbow_cli_path']
        ldap_project_id = self.config_section_map("SectionFour")['ldap_project_id']
        ldap_user_id = self.config_section_map("SectionFour")['ldap_user_id']
        ldap_password = self.config_section_map("SectionFour")['ldap_password']
        ldap_auth_url = self.config_section_map("SectionFour")['ldap_auth_url']
        ldap_token = subprocess.check_output(['bash', fogbow_cli_path, 'token', '--create',
                                              '-DprojectId=' + ldap_project_id, '-DuserId=' + ldap_user_id,
                                              '-Dpassword=' + ldap_password, '-DauthUrl=' + ldap_auth_url, '--type',
                                              'openstack']).strip()
        return ldap_token

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

    def get_auth_token(self):
        return self.__auth_token

    def get_storage_url(self):
        return self.__storage_url

    def get_container_name(self):
        return self.__container_name

    def get_total_disk(self):
        return self.__total_disk
