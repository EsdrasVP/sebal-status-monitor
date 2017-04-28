class ApplicationConstants:

    def __init__(self):
        pass

    # Monitor constants
    DEFAULT_MONITOR_SLEEP_TIME = 3600

    # Status implementation types
    CACHET_TYPE = "cachet"

    # Components constants
    SCHEDULER_COMPONENT = "scheduler"
    CRAWLER_COMPONENT = "crawler"
    FETCHER_COMPONENT = "fetcher"

    # Database state constants
    DEFAULT_PROCESSED_STATE = "fetched"
    DEFAULT_DOWNLOADED_STATE = "downloaded"
    DEFAULT_SUBMITTED_STATE = "submitted"

    # Metric constants
    PROCESSED_IMAGES_METRIC_NAME = "Processed Images"
    SUBMITTED_IMAGES_METRIC_NAME = "Submitted Images"
    DOWNLOADED_IMAGES_METRIC_NAME = "Downloaded Images"
    AVG_EXECUTION_TIME_METRIC_NAME = "Average Execution Time"

    # Configuration constants
    DEFAULT_CONFIG_FILE_PATH = "resources/config.ini"  # TODO: see if this path is correct
    DEFAULT_STATUS_IMPLEMENTATION = "cachet"
