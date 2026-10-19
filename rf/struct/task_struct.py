import copy
import time

import rf.utils.tools as tools


class Task:
    """Describe a callback, its keyword arguments, and optional routing overrides."""

    __TASK_ATTRS__ = {"callback", "meta", "process_name", "taskid"}

    def __init__(self, callback, meta, process_name=None, taskid=None, **kwargs):
        self.callback = callback
        self.meta = meta
        self.process_name = process_name
        self.priority = time.time()
        self.taskid = taskid or tools.get_uuid(
            namespace=f"{process_name}.{callback}", name=str(meta)
        )
        self.redis_key = kwargs.get("redis_key")
        self.redis_ip_ports = kwargs.get("redis_ip_ports")
        self.redis_user_pass = kwargs.get("redis_user_pass")
        self.redis_db = kwargs.get("redis_db")

    @property
    def callback_name(self):
        """Return a callable name or the stored callback string."""
        return (
            getattr(self.callback, "__name__")
            if callable(self.callback)
            else self.callback
        )

    @property
    def to_dict(self):
        """Create a wire payload without changing this task.

        Only task identity and callback data are persisted. Legacy nonprimitive
        values use pickle; normal Redis JSON payloads must be JSON-compatible.
        """
        request_dict = {}
        for key, value in self.__dict__.items():
            if value is not None and key in self.__class__.__TASK_ATTRS__:
                if key == "callback":
                    value = self.callback_name
                if not isinstance(
                    value, (bytes, bool, float, int, str, tuple, list, dict)
                ):
                    value = tools.dumps_obj(value)

                request_dict[key] = value
        return request_dict

    @classmethod
    def from_dict(cls, request_dict):
        """Build a task without modifying the input mapping.

        Legacy byte values are unpickled and must come from trusted input.
        """
        values = {
            key: tools.loads_obj(value) if isinstance(value, bytes) else value
            for key, value in request_dict.items()
        }
        return cls(**values)

    def copy(self):
        """Copy persisted task fields deeply; reset transient routing and priority."""
        return self.__class__.from_dict(copy.deepcopy(self.to_dict))
