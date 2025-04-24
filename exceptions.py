class SchedulerError(Exception):
    """Custom exception for Scheduler errors"""
    pass

class ConfigError(SchedulerError):
    """Exception raised for errors in the configuration."""
    pass

class DataError(SchedulerError):
    """Exception raised for errors in input data (workers, holidays, etc.)."""
    pass
