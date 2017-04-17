class ApplicationConstants:

    # Monitor constants
    DEFAULT_MONITOR_SLEEP_TIME = 3600

    # Components constants
    SCHEDULER_COMPONENT = "scheduler"
    CRAWLER_COMPONENT = "crawler"
    FETCHER_COMPONENT = "fetcher"

    # Database state constants
    DEFAULT_PROCESSED_STATE = "fetched"
    DEFAULT_DOWNLOADED_STATE = "downloaded"
    DEFAULT_SUBMITTED_STATE = "submitted"

    # Configuration constants
    DEFAULT_CONFIG_FILE_PATH = "resources/config.ini"  # TODO: see if this path is correct
    DEFAULT_STATUS_IMPLEMENTATION = "cachet"
