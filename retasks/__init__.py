__version__ = '0.0.1a'

from pickle import dumps
from uuid import uuid4

broker = None


class Task(object):
    def run(self, *args, **kwargs):
        raise NotImplementedError()

    def apply_async(self, *args, **kwargs):
        task_id = uuid4()
        task_tuple = (self.run, args, kwargs)
        task_pack = dumps(task_tuple)
        broker.new_task(task_id, task_pack)
        return task_id
