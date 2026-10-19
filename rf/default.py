"""Default settings with optional overrides from an application settings module."""

import importlib
import math
import os

# Redis connection and task key templates.
# Legacy lists and comma-separated endpoints use only the first host.
REDIS_IP_PORTS = os.getenv("REDIS_IP_PORTS", "127.0.0.1:6379")
REDIS_USER_PASS = os.getenv("REDIS_USER_PASS")
REDIS_DB = int(os.getenv("REDIS_DB", 0))
REDIS_KEY = ""
TAB_JOB = "rf:job:{redis_key}"
TAB_JOB_ERR = "rf:err:{redis_key}"
DELETE_KEYS = ()

# Worker lifecycle and concurrency.
KEEP_ALIVE = False
PROCESS_AUTO_START_JOBS = True
PROCESS_THREAD_COUNT = 10

# Bound prefetch and expire leases after heartbeat loss.
COLLECTOR_TASK_COUNT = 20
JOB_LOST_TIMEOUT = 30  # Lease duration in seconds.
HEARTBEAT_INTERVAL = 5
HEARTBEAT_BATCH_SIZE = 1000
REDIS_SOCKET_TIMEOUT = 5
REDIS_CONNECT_TIMEOUT = 5

# Count outputs, errors, and completion records, including failed batches.
BUFFER_MAX_RECORDS = 1000

# Logging defaults.
LOG_NAME = os.path.basename(os.getcwd())
LOG_PATH = "log/%s.log" % LOG_NAME
LOG_LEVEL = os.getenv("LOG_LEVEL", "DEBUG")
LOG_COLOR = True
LOG_IS_WRITE_TO_CONSOLE = True
LOG_IS_WRITE_TO_FILE = False
LOG_MODE = "w"
LOG_MAX_BYTES = 10 * 1024 * 1024
LOG_BACKUP_COUNT = 20
LOG_ENCODING = "utf8"

PRINT_EXCEPTION_DETAILS = True

LOG_FORMAT = "%(threadName)s|%(asctime)s|%(filename)s|%(funcName)s|line:%(lineno)d|%(levelname)s| %(message)s"

os.environ["LOGURU_FORMAT"] = (
    "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
    "<level>{level: <8}</level> | "
    "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>line:{line}</cyan> | <level>{message}</level>"
)
# Historical spelling retained for configuration compatibility.
OTHERS_LOG_LEVAL = "ERROR"


def _load_settings():
    """Load application overrides, ignoring only an absent settings module."""
    try:
        module = importlib.import_module("settings")
    except ModuleNotFoundError as exc:
        if exc.name != "settings":
            raise
        return {}
    names = getattr(module, "__all__", None)
    if names is None:
        names = (name for name in vars(module) if not name.startswith("_"))
    return {name: getattr(module, name) for name in names}


globals().update(_load_settings())


def validate_runtime_settings():
    """Reject unsafe lease timing and invalid capacity settings before startup."""
    for name in (
        "BUFFER_MAX_RECORDS",
        "HEARTBEAT_BATCH_SIZE",
        "COLLECTOR_TASK_COUNT",
        "PROCESS_THREAD_COUNT",
    ):
        value = globals()[name]
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ValueError(f"{name} must be a positive integer.")
    for name in (
        "JOB_LOST_TIMEOUT",
        "HEARTBEAT_INTERVAL",
        "REDIS_SOCKET_TIMEOUT",
        "REDIS_CONNECT_TIMEOUT",
    ):
        value = globals()[name]
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            or value <= 0
        ):
            raise ValueError(f"{name} must be a finite positive number.")
    minimum = 2 * (HEARTBEAT_INTERVAL + REDIS_SOCKET_TIMEOUT + REDIS_CONNECT_TIMEOUT)
    if JOB_LOST_TIMEOUT < minimum:
        raise ValueError(
            "JOB_LOST_TIMEOUT must be at least twice the sum of "
            "HEARTBEAT_INTERVAL, REDIS_SOCKET_TIMEOUT, and REDIS_CONNECT_TIMEOUT."
        )
