import logging
import psycopg2
import time
from time import strftime
from datetime import datetime, timedelta


class Monitor:

    DEFAULT_MONITOR_SLEEP_TIME = 3600
    DEFAULT_STATUS_IMPLEMENTATION = "cachet"

    def __init__(self, db_name, db_user, db_password, db_host, db_port, db_images_table_name, status_implementation,
                 cachet_host_url):
        self.__db_name = db_name
        self.__db_user = db_user
        self.__db_password = db_password
        self.__db_host = db_host
        self.__db_port = db_port
        self.__db_images_table_name = db_images_table_name
        self.__status_implementation = status_implementation
        self.__cachet_host_url = cachet_host_url

    def start(self):
        date_least_one_hour = datetime.today() - timedelta(hours=1)
        date = strftime("%Y-%m-%d %H", date_least_one_hour)
        number_of_processed_images = self.get_processed_images_last_hour(date)
        self.handle_processed_images(number_of_processed_images)
        time.sleep(self.DEFAULT_MONITOR_SLEEP_TIME)

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
            logging.error("error while getting number of processed images", e)
            return e.pgcode

    def handle_processed_images(self, number_of_processed_images):
        if self.__status_implementation == self.DEFAULT_STATUS_IMPLEMENTATION:
            self.update_image_number_cachet(number_of_processed_images)

    def update_image_number_cachet(number_of_processed_images):
        # TODO: call cachet POST
        return None
