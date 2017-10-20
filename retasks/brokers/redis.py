from retasks.brokers import Broker
import redis


class RedisBroker(Broker):
    def __init__(self, host='localhost', port=6379, db=0):
        self._connection = redis.StrictRedis(host=host, port=port, db=db)

    def _get_task_key(self, task_id):
        return 'task_{}'.format(task_id)

    def _get_worker_key(self, worker_id):
        return 'worker_{}'.format(worker_id)

    def _get_task_pack_by_task_key(self, task_key):
        return self._connection.get(task_key)

    def _get_keys_by_worker_id(self, worker_id):
        worker_key = self._get_worker_key(worker_id)
        task_key = self._connection.get(worker_key)
        return worker_key, task_key

    def new_task(self, task_id, task_pack):
        task_key = self._get_task_key(task_id)

        if self._get_task_pack_by_task_key(task_key):
            return

        with self._connection.pipeline() as pipeline:
            pipeline.set(task_key, task_pack)
            pipeline.lpush('task_queue', task_key)
            pipeline.execute()

    def pull_task(self, worker_id):
        worker_key, task_key = self._get_keys_by_worker_id(worker_id)

        if task_key:
            return self._get_task_pack_by_task_key(task_key)

        task_key = self._connection.rpop('task_queue')

        if not task_key:
            return None

        self._connection.set(worker_key, task_key)

        return self._get_task_pack_by_task_key(task_key)

    def finish_task(self, worker_id):
        worker_key, task_key = self._get_keys_by_worker_id(worker_id)

        self._connection.delete(worker_key)
        self._connection.delete(task_key)

    def terminate_worker(self, worker_id):
        worker_key, task_key = self._get_keys_by_worker_id(worker_id)

        if task_key:
            self._connection.rpush(task_key)

