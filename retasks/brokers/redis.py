import redis

from retasks.brokers import Broker


class RedisBroker(Broker):
    def __init__(self, host='localhost', port=6379, db=0):
        self._connection = redis.StrictRedis(host=host, port=port, db=db)

    def _get_task_key(self, task_id):
        return 'task_{}'.format(task_id)

    def _get_worker_key(self, worker_id):
        return 'worker_{}'.format(worker_id)

    def _get_worker_check_key(self, worker_id):
        worker_key = self._get_worker_key(worker_id)
        return self._get_worker_check_key_by_worker_key(worker_key)

    def _get_worker_check_key_by_worker_key(self, worker_key):
        return 'check_{}'.format(worker_key)

    def _get_task_pack_by_task_key(self, task_key):
        return self._connection.get(task_key)

    def _get_keys_by_worker_id(self, worker_id):
        worker_key = self._get_worker_key(worker_id)
        task_key = self._connection.get(worker_key)
        return worker_key, task_key

    def _pop_task_key_from_queue(self):
        return self._connection.rpop('task_queue')

    def _add_task_key_to_queue(self, task_key):
        return self._connection.lpush('task_queue', task_key)

    def _readd_task_key_to_queue(self, worker_key):
        task_key = self._connection.get(worker_key)
        if task_key:
            self._add_task_key_to_queue(task_key)
            self._connection.delete(worker_key)
            return True
        else:
            return False

    def _readd_failed_workers(self):
        for worker_key in self._connection.keys('worker_*'):
            worker_check_key = self._get_worker_check_key_by_worker_key(worker_key)
            if not self._connection.exists(worker_check_key):
                self._readd_task_key_to_queue(worker_key)

    def new_task(self, task_id, task_pack):
        task_key = self._get_task_key(task_id)

        if self._get_task_pack_by_task_key(task_key):
            return

        self._connection.set(task_key, task_pack)
        self._add_task_key_to_queue(task_key)

    def pull_task(self, worker_id):
        worker_key, task_key = self._get_keys_by_worker_id(worker_id)

        if task_key:
            return self._get_task_pack_by_task_key(task_key)

        # check if there are not some abandoned tasks and readd them to queue
        self._readd_failed_workers()

        task_key = self._pop_task_key_from_queue()

        if not task_key:
            return None

        worker_check_key = self._get_worker_check_key(worker_id)
        self._connection.set(worker_check_key, 1, ex=30)

        self._connection.set(worker_key, task_key)

        return self._get_task_pack_by_task_key(task_key)

    def finish_task(self, worker_id):
        # FIXME solve situation, if some worker is not checked but try finish self

        worker_key, task_key = self._get_keys_by_worker_id(worker_id)

        if not task_key:
            # broken protocol
            return False

        self._connection.delete(worker_key)
        self._connection.delete(task_key)
        return True

    def check_worker(self, worker_id):
        worker_check_key = self._get_worker_check_key(worker_id)
        worker_check_key_exists = bool(self._connection.expire(worker_check_key, 30))
        return worker_check_key_exists

    def terminate_worker(self, worker_id):
        worker_key = self._get_worker_key(worker_id)
        return self._readd_task_key_to_queue(worker_key)
