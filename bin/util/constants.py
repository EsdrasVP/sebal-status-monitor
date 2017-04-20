class ApplicationConstants:

    def __init__(self):
        pass

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

    # Cachet constants
    COMPONENTS_ENDPOINT = "api/v1/components"
    COMPONENT_GROUPS_ENDPOINT = "api/v1/components/groups"
    INCIDENTS_ENDPOINT = "api/v1/incidents"
    METRICS_ENDPOINT = "api/v1/metrics"

    # Configuration constants
    DEFAULT_CONFIG_FILE_PATH = "resources/config.ini"  # TODO: see if this path is correct
    DEFAULT_STATUS_IMPLEMENTATION = "cachet"
