from time import sleep

from retasks import Task


class TestTask(Task):
    def run(self, *args, **kwargs):
        sleep(15)
        return args, kwargs
