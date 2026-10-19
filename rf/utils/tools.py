import pickle
import time
import uuid

from rf.utils.log import log


def get_uuid(name: str = "", namespace: str = ""):
    """Return a deterministic UUID3 for a nonempty name, or a fresh UUID1 otherwise."""
    if namespace:
        namespace_id = uuid.uuid3(uuid.NAMESPACE_URL, namespace)
    else:
        namespace_id = uuid.NAMESPACE_URL
    if name:
        u = str(uuid.uuid3(namespace_id, name))
    else:
        u = str(uuid.uuid1())
    return u


def dumps_obj(obj):
    """Serialize an object with the highest supported pickle protocol."""
    return pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)


def loads_obj(obj_str):
    """Deserialize a trusted pickle payload. Never pass untrusted bytes."""
    return pickle.loads(obj_str)


def get_method(obj, name):
    """Look up a named attribute, logging and returning None when absent."""
    name = str(name)
    try:
        return getattr(obj, name)
    except AttributeError:
        log.error("Cannot find method %r on %s.", name, obj)
        return None


def get_current_timestamp():
    """Return the current Unix timestamp truncated to whole seconds."""
    return int(time.time())


def delay_time(sleep_time=60):
    """Sleep for sleep_time seconds, defaulting to one minute."""
    time.sleep(sleep_time)
