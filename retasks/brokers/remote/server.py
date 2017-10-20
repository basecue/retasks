from multiprocessing.connection import Listener
from threading import Thread
from logging import getLogger


logger = getLogger(__name__)


class RemoteBrokerServer(object):
    def serve_forever(self):
        while True:
            client_connection = self._server.accept()
            t = Thread(target=self._handle_client, args=(client_connection,))
            t.daemon = True
            t.start()

    def _handle_client(self, client_connection):
        while True:
            message = client_connection.recv()
            try:
                result = self.on_message(message)

                client_connection.send(result)
            except Exception as error:
                client_connection.send(error)

    def __init__(self, broker, address, authkey=None):
        self._broker = broker
        self._server = Listener(address, authkey=authkey)

    def on_message(self, message):
        func_name = message[0]
        if func_name.startswith('_'):
            raise AttributeError('')  # FIXME

        args = message[1:]
        return getattr(self._broker, func_name)(*args)
