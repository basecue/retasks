from retasks.brokers.redis import RedisBroker
import retasks
from .tasks import TestTask


retasks.broker = RedisBroker()
test_task = TestTask()


def test(*args, **kwargs):
    test_task.apply_async(*args, **kwargs)
