"""Configure console logging and numbered rotating log files.

Original author: Boris <boris_liu@foxmail.com>, 2018.
"""

import logging
import os
import sys
from logging.handlers import BaseRotatingHandler

import loguru
from better_exceptions import format_exception

import rf.default as setting


class InterceptHandler(logging.Handler):
    """Forward standard logging records to loguru for colored console output."""

    def emit(self, record):
        # Skip the standard logging frames when forwarding caller information.
        logger_opt = loguru.logger.opt(depth=6, exception=record.exc_info)
        logger_opt.log(record.levelname, record.getMessage())


class RotatingFileHandler(BaseRotatingHandler):
    """Rotate logs with zero-padded backup numbers before the extension."""

    def __init__(
        self, filename, mode="a", max_bytes=0, backup_count=0, encoding=None, delay=0
    ):
        BaseRotatingHandler.__init__(self, filename, mode, encoding, delay)
        self.max_bytes = max_bytes
        self.backup_count = backup_count
        self.placeholder = str(len(str(backup_count)))

    def doRollover(self):
        if self.stream:
            self.stream.close()
            self.stream = None
        if self.backup_count > 0:
            for i in range(self.backup_count - 1, 0, -1):
                source = self._backup_filename(i)
                destination = self._backup_filename(i + 1)
                if os.path.exists(source):
                    os.replace(source, destination)
            destination = self._backup_filename(1)
            if os.path.exists(destination):
                os.remove(destination)
            # Delayed opening can leave no active file to rename.
            if os.path.exists(self.baseFilename):
                os.rename(self.baseFilename, destination)
        if not self.delay:
            self.stream = self._open()

    def _backup_filename(self, index):
        """Insert a padded index before the final extension, preserving parent paths."""
        stem, extension = os.path.splitext(self.baseFilename)
        return f"{stem}{index:0{self.placeholder}d}{extension}"

    def shouldRollover(self, record):
        if self.stream is None:
            self.stream = self._open()
        if self.max_bytes > 0:
            msg = "%s\n" % self.format(record)
            # Seek explicitly for append-mode streams on Windows.
            self.stream.seek(0, 2)
            if self.stream.tell() + len(msg) >= self.max_bytes:
                return 1
        return 0


def get_logger(
    name=None,
    path=None,
    log_level=None,
    is_write_to_console=None,
    is_write_to_file=None,
    color=None,
    mode=None,
    max_bytes=None,
    backup_count=None,
    encoding=None,
):
    """Configure a named logger, using rf.default for unspecified options.

    Console and file output can be enabled independently. File rotation uses
    max_bytes as the size limit and backup_count as the number of retained files.
    """
    # Read settings lazily so application overrides are visible.
    name = name or setting.LOG_NAME
    path = path or setting.LOG_PATH
    log_level = log_level or setting.LOG_LEVEL
    is_write_to_console = (
        is_write_to_console
        if is_write_to_console is not None
        else setting.LOG_IS_WRITE_TO_CONSOLE
    )
    is_write_to_file = (
        is_write_to_file
        if is_write_to_file is not None
        else setting.LOG_IS_WRITE_TO_FILE
    )
    color = color if color is not None else setting.LOG_COLOR
    mode = mode or setting.LOG_MODE
    max_bytes = setting.LOG_MAX_BYTES if max_bytes is None else max_bytes
    backup_count = setting.LOG_BACKUP_COUNT if backup_count is None else backup_count
    encoding = encoding or setting.LOG_ENCODING

    name = name.split(os.sep)[-1].split(".")[0]

    logger = logging.getLogger(name)
    logger.setLevel(log_level)

    formatter = logging.Formatter(setting.LOG_FORMAT)
    if setting.PRINT_EXCEPTION_DETAILS:
        formatter.formatException = lambda exc_info: format_exception(*exc_info)

    if is_write_to_file:
        directory = os.path.dirname(path)
        if directory:
            os.makedirs(directory, exist_ok=True)

        rf_handler = RotatingFileHandler(
            path,
            mode=mode,
            max_bytes=max_bytes,
            backup_count=backup_count,
            encoding=encoding,
        )
        rf_handler.setFormatter(formatter)
        logger.addHandler(rf_handler)
    if color and is_write_to_console:
        loguru_handler = InterceptHandler()
        loguru_handler.setFormatter(formatter)

        logger.addHandler(loguru_handler)
    elif is_write_to_console:
        stream_handler = logging.StreamHandler()
        stream_handler.stream = sys.stdout
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    _handler_list = []
    _handler_name_list = []
    # Keep one handler for each configured destination.
    for _handler in logger.handlers:
        if str(_handler) not in _handler_name_list:
            _handler_name_list.append(str(_handler))
            _handler_list.append(_handler)
    logger.handlers = _handler_list
    return logger


# Apply the configured level to common third-party loggers.
STOP_LOGS = [
    "urllib3.response",
    "urllib3.connection",
    "elasticsearch.trace",
    "requests.packages.urllib3.util",
    "requests.packages.urllib3.util.retry",
    "urllib3.util",
    "requests.packages.urllib3.response",
    "requests.packages.urllib3.contrib.pyopenssl",
    "requests.packages",
    "urllib3.util.retry",
    "requests.packages.urllib3.contrib",
    "requests.packages.urllib3.connectionpool",
    "requests.packages.urllib3.poolmanager",
    "urllib3.connectionpool",
    "requests.packages.urllib3.connection",
    "elasticsearch",
    "log_request_fail",
    "requests",
    "selenium.webdriver.remote.remote_connection",
    "selenium.webdriver.remote",
    "selenium.webdriver",
    "selenium",
    "MARKDOWN",
    "build_extension",
    "calculate_area",
    "largest_image_url",
    "newspaper.images",
    "newspaper",
    "Importing",
    "PIL",
]


OTHERS_LOG_LEVAL = getattr(logging, setting.OTHERS_LOG_LEVAL)
for STOP_LOG in STOP_LOGS:
    logging.getLogger(STOP_LOG).setLevel(OTHERS_LOG_LEVAL)


class Log:
    """Initialize the shared logger lazily on first use."""

    log = None

    def _logger(self):
        if self.__class__.log is None:
            self.__class__.log = get_logger()
        return self.__class__.log

    def __getattr__(self, name):
        return getattr(self._logger(), name)

    @property
    def debug(self):
        return self._logger().debug

    @property
    def info(self):
        return self._logger().info

    @property
    def warning(self):
        return self._logger().warning

    @property
    def exception(self):
        return self._logger().exception

    @property
    def error(self):
        return self._logger().error

    @property
    def critical(self):
        return self._logger().critical


log = Log()
