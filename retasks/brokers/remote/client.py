from multiprocessing.connection import Client
from time import sleep

from logging import getLogger

from retasks.brokers import Broker

logger = getLogger(__name__)


class RemoteConnection(object):
    def __init__(self, address, authkey=None):
        self.address = address
        self.authkey = authkey
        self.__connection = None

    @property
    def _connection(self):
        while self.__connection is None:
            try:
                self.__connection = Client(self.address, authkey=self.authkey)
            except ConnectionRefusedError as error:
                logger.warning(error)
                # FIXME
                sleep(0.1)

        return self.__connection

    def _request(self, message):
        while True:
            try:
                self._connection.send(message)
                return self._connection.recv()
            except Exception as error:
                logger.warning(error)
                self.__connection = None  # reconnect

    def send(self, message):
        while True:
            try:
                result = self._request(message)
            except Exception as error:
                logger.warning(error)
                pass
            else:
                break

        if isinstance(result, Exception):
            raise result

        return result


class RemoteBroker(Broker):
    def __init__(self, address, authkey=None):
        self._connection = RemoteConnection(address, authkey=authkey)

    def new_task(self, task_id, task_pack):
        self._connection.send(('new_task', task_id, task_pack))

    def pull_task(self, worker_id):
        return self._connection.send(('pull_task', worker_id))

    def finish_task(self, worker_id):
        self._connection.send(('finish_task', worker_id))

    def terminate_worker(self, worker_id):
        self._connection.send(('terminate_worker', worker_id))
