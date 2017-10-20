class Broker(object):
    def new_task(self, task_id, task_tuple):
        raise NotImplementedError()

    def pull_task(self, worker_id):
        raise NotImplementedError()

    def finish_task(self, worker_id):
        raise NotImplementedError()

    def terminate_worker(self, worker_id):
        raise NotImplementedError()
