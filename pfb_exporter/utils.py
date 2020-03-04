import datetime
import logging
import logging.handlers
import importlib
import time
import os

from pfb_exporter.config import (
    DEFAULT_LOG_FILENAME,
    DEFAULT_LOG_LEVEL,
    DEFAULT_LOG_OVERWRITE_OPT,
)


DEFAULT_FORMAT = (
    "%(asctime)s - %(name)s"
    " - Thread: %(threadName)s - %(levelname)s - %(message)s"
)
DEFAULT_FORMATTER = logging.Formatter(DEFAULT_FORMAT)


def setup_logger(
    log_dir,
    overwrite_log=DEFAULT_LOG_OVERWRITE_OPT,
    log_level=DEFAULT_LOG_LEVEL,
):
    """
    Configure and create the logger

    :param log_dir: the path to the log directory
    :param overwrite_log: a boolean specifying whether to create new log files
    or overwrite a defaul log file 'ingest.log'
    :param log_level: a string specifying what level of log messages to record
    in the log file. Values are not case sensitive. The list of acceptable
    values are the names of Python's standard lib logging levels.
    (critical, error, warning, info, debug, notset)
    """
    # Default file name
    filename = DEFAULT_LOG_FILENAME

    # Create a new log file named with a timestamp
    if not overwrite_log:
        filename = "pfb-export-{}.log".format(timestamp())

    os.makedirs(log_dir, exist_ok=True)
    log_filepath = os.path.join(log_dir, filename)

    # Setup rotating file handler
    fileHandler = logging.handlers.RotatingFileHandler(log_filepath, mode="w")
    fileHandler.setFormatter(DEFAULT_FORMATTER)

    # Setup console handler
    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(DEFAULT_FORMATTER)

    # Set log level and handlers
    root = logging.getLogger()
    root.setLevel(log_level)
    root.addHandler(fileHandler)
    root.addHandler(consoleHandler)

    return log_filepath


def timestamp():
    """
    Helper to create an ISO 8601 formatted string that represents local time
    and includes the timezone info.
    """
    # Calculate the offset taking into account daylight saving time
    # https://stackoverflow.com/questions/2150739/iso-time-iso-8601-in-python
    if time.localtime().tm_isdst:
        utc_offset_sec = time.altzone
    else:
        utc_offset_sec = time.timezone
    utc_offset = datetime.timedelta(seconds=-utc_offset_sec)
    t = (
        datetime.datetime.now()
        .replace(tzinfo=datetime.timezone(offset=utc_offset))
        .isoformat()
    )

    return str(t)


def seconds_to_hms(t):
    """
    Convert t in seconds to an hh:mm:ss formatted string

    :param t: time value in seconds
    :type t: int or float

    :returns: hh:mm:ss formatted time string
    """
    min, sec = divmod(t, 60)
    hour, min = divmod(min, 60)

    return '{:0>2}:{:0>2}:{:0>2}'.format(int(hour), int(min), int(sec))


def import_module_from_file(filepath):
    """
    Import a Python module given a filepath
    """
    module_name = os.path.basename(filepath).split(".")[0]
    spec = importlib.util.spec_from_file_location(module_name, filepath)
    imported_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(imported_module)
    return imported_module
