class ApplicationConstants:

    def __init__(self):
        pass

    # Monitor constants
    DEFAULT_MONITOR_SLEEP_TIME = 3600

    # Status implementation types
    CACHET_TYPE = "cachet"

    # Components constants
    SCHEDULER_COMPONENT = "Scheduler"
    CRAWLER_COMPONENT = "Crawler"
    FETCHER_COMPONENT = "Fetcher"

    # Database state constants
    DEFAULT_PROCESSED_STATE = "fetched"
    DEFAULT_DOWNLOADED_STATE = "downloaded"
    DEFAULT_SUBMITTED_STATE = "submitted"

    # Metric constants
    PROCESSED_IMAGES_METRIC_NAME = "Processed Images"
    SUBMITTED_IMAGES_METRIC_NAME = "Submitted Images"
    DOWNLOADED_IMAGES_METRIC_NAME = "Downloaded Images"
    AVG_EXECUTION_TIME_METRIC_NAME = "Average Execution Time"
    CRAWLER_DISK_USAGE_METRIC_NAME = "Crawler Disk Usage"
    SWIFT_DISK_USAGE_METRIC_NAME = "Swift Disk Usage"

    # Configuration constants
    DEFAULT_CONFIG_FILE_PATH = "resources/config.ini"  # TODO: see if this path is correct
    DEFAULT_STATUS_IMPLEMENTATION = "cachet"
