"""Route a task to a full Redis queue key and an optional remote connection."""

import json

from ..db.redisdb import RedisDB
from .task_struct import Task as Task_struct


class Queue:
    """Describe an outbound task destination. redis_key is the full sorted-set key."""

    __TASK_ATTRS__ = {
        "RedisDB",
        "redis_key",
        "Task",
    }

    def __init__(
        self,
        redis_key,
        task: Task_struct,
        redis_ip_ports=None,
        redis_user_pass=None,
        redis_db=0,
    ):
        self.redis_key = redis_key
        self.Task = task
        self.RedisDB = (
            RedisDB(ip_ports=redis_ip_ports, user_pass=redis_user_pass, db=redis_db)
            if redis_ip_ports
            else None
        )

    @property
    def to_dict(self):
        """Return the legacy destination mapping with a serialized task and score."""
        task_dict = json.dumps(self.Task.to_dict, ensure_ascii=False)
        return {
            self.RedisDB: {
                self.redis_key: {
                    "job_list": [task_dict],
                    "prioritys": [self.Task.priority],
                }
            }
        }
