import logging
import ConfigParser
import psycopg2
import time
from time import strftime
from datetime import datetime, timedelta


# Configuration Variables
config_file_path = None
config = None
db_name = None
db_user = None
db_password = None
db_host = None
db_port = None
db_images_table_name = None
status_implementation = None
cachet_host_url = None


# Constants
DEFAULT_MONITOR_SLEEP_TIME = 3600
DEFAULT_CONFIG_FILE_PATH = "resources/config.ini"
DEFAULT_STATUS_IMPLEMENTATION = "cachet"


def get_processed_images_last_hour(date_prefix):
    global db_name, db_user, db_password, db_host, db_port, db_images_table_name
    try:
        connection = psycopg2.connect(db_name, db_user, db_password, db_host, db_port, sslmode='verify-full')
        cursor = connection.cursor()

        # Date prefix must follow an established format
        # ex.: 2017-04-12 18 (date previous_hour)
        statement_sql = "SELECT * FROM " + db_images_table_name + \
                        " WHERE state = 'fetched' AND utime::text LIKE '" + date_prefix + "%';"
        cursor.execute(statement_sql)
        return cursor.rowcount
    except psycopg2.Error as e:
        return e.pgcode


def update_image_number_cachet(number_of_processed_images):
    # TODO: call cachet POST
    return None


def handle_processed_images(number_of_processed_images):
    if status_implementation == DEFAULT_STATUS_IMPLEMENTATION:
        update_image_number_cachet(number_of_processed_images)


def config_section_map(section):
    dict1 = {}
    options = config.options(section)
    for option in options:
        try:
            dict1[option] = config.get(section, option)
            if dict1[option] == -1:
                logging.debug("skip: %s", option)
        except:
            logging.error("exception on %s!", option)
            dict1[option] = None
    return dict1


def monitor():
    date_least_one_hour = datetime.today() - timedelta(hours=1)
    date = strftime("%Y-%m-%d %H", date_least_one_hour)
    number_of_processed_images = get_processed_images_last_hour(date)
    handle_processed_images(number_of_processed_images)


def assign_db_variable_values():
    global db_name, db_user, db_password, db_host, db_port, db_images_table_name, status_implementation, cachet_host_url
    db_name = config_section_map("SectionOne")['db_name']
    db_user = config_section_map("SectionOne")['db_user']
    db_password = config_section_map("SectionOne")['db_password']
    db_host = config_section_map("SectionOne")['db_host']
    db_port = config_section_map("SectionOne")['db_port']
    db_images_table_name = config_section_map("SectionOne")['db_images_table_name']
    status_implementation = config_section_map("SectionOne")['status_implementation']
    cachet_host_url = config_section_map("SectionOne")['cachet_host_url']


def init():
    global config
    config = ConfigParser.ConfigParser()
    config.read(DEFAULT_CONFIG_FILE_PATH)
    assign_db_variable_values()

    while True:
        monitor()
        time.sleep(DEFAULT_MONITOR_SLEEP_TIME)


init()