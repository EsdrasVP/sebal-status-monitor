import logging
import ConfigParser
#from monitor import Monitor


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
DEFAULT_CONFIG_FILE_PATH = "resources/config.ini"


def config_section_map(section):
    dict1 = {}
    options = config.options(section)
    for option in options:
        try:
            dict1[option] = config.get(section, option)
            if dict1[option] == -1:
                logging.debug("skip: %s", option)
        except Exception as e:
            logging.debug(str(e))
            dict1[option] = None
    return dict1


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
    global config, db_user, db_password, db_host, db_port, db_images_table_name, status_implementation, cachet_host_url
    config = ConfigParser.ConfigParser()
    config.read(DEFAULT_CONFIG_FILE_PATH)
    assign_db_variable_values()
#    status_monitor = Monitor.__init__(db_user, db_password, db_host, db_port, db_images_table_name,
#                                      status_implementation, cachet_host_url)
#    status_monitor.start()


init()