import logging
from datetime import timedelta, datetime
from importlib import import_module
from logging import getLogger
from pickle import loads
import os
import signal

from multiprocessing import Process
from time import sleep

import sys

logger = getLogger(__name__)

logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
logger.addHandler(ch)


def enough_time(start_time, timeout):
    return (timeout is None) or (start_time + timedelta(seconds=timeout) > datetime.now())


class WorkerSupervisor(object):
    def __init__(self, workers_num, broker_path, application_path, stop_timeout=30):
        broker_modulepath, broker_classname = broker_path.rsplit('.', 1)
        broker_module = import_module(broker_modulepath)
        broker_cls = getattr(broker_module, broker_classname)

        self.stop_timeout = stop_timeout
        self.broker_cls = broker_cls
        self.broker = None
        self.application_path = application_path
        self.workers_num = workers_num
        self.workers_processes = []

    def receive_hup_signal(self, signum, stack):
        logger.info(u'Worker supervisor received HUP signal')
        self.gracefully_stop_workers()

    def receive_term_signal(self, signum, stack):
        logger.info(u'Worker supervisor received TERM signal')
        self.gracefully_stop_workers()
        sys.exit(0)

    def receive_int_signal(self, signum, stack):
        self.gracefully_stop_workers(timeout=self.stop_timeout)

        if not self.workers_processes:
            sys.exit(0)

        self.stop_workers()
        sys.exit(1)

    def receive_quit_signal(self, signum, stack):
        self.stop_workers()
        sys.exit(2)

    def gracefully_stop_workers(self, timeout=None):
        start_time = datetime.now()

        for worker, process in self.workers_processes:
            process.terminate()

        while self.workers_processes and enough_time(start_time, timeout):
            self.check_workers()

    def stop_workers(self):
        for worker, process in self.workers_processes:
            if process.is_alive():
                process.kill()

        self.check_workers()

    def check_workers(self):
        for worker, process in self.workers_processes:
            if not process.is_alive():
                self.broker.terminate_worker(process.pid)
                logger.debug("Worker process terminated: id={}".format(process.pid))
                self.workers_processes.remove((worker, process))

    def start(self):

        signal.signal(signal.SIGHUP, self.receive_hup_signal)
        signal.signal(signal.SIGTERM, self.receive_term_signal)
        signal.signal(signal.SIGINT, self.receive_int_signal)
        signal.signal(signal.SIGQUIT, self.receive_quit_signal)

        logger.debug("Worker supervisor started: application={self.application_path} broker_class={self.broker_cls}".format(self=self))

        self.broker = self.broker_cls()

        #TODO terminate all zombie workers / notify broker about termination

        while True:
            workers_processes_fill = self.workers_num - len(self.workers_processes)

            for i in range(workers_processes_fill):
                worker = Worker(broker=self.broker, application_path=self.application_path)
                process = Process(target=worker.start)
                process.start()
                logger.debug("Worker process started: id={process.pid}".format(process=process))
                self.workers_processes.append((worker, process))

            self.check_workers()

            sleep(0.5)


class Worker(object):
    def __init__(self, broker, application_path=None):
        self.application_path = application_path
        self.running = True
        self.broker = broker
        self.id = None

    def run_task(self, task, task_args, task_kwargs):
        logger.debug('Worker run task {} {} {}'.format(task, task_args, task_kwargs))
        try:
            return task(*task_args, **task_kwargs)
        except Exception as exception:
            return exception

    def receive_signal(self, signum, stack):
        logger.info(u'Worker {} received signal: {}'.format(self.id, signum))
        self.running = False

    def start(self):
        signal.signal(signal.SIGHUP, self.receive_signal)
        signal.signal(signal.SIGTERM, self.receive_signal)
        signal.signal(signal.SIGINT, self.receive_signal)
        signal.signal(signal.SIGQUIT, self.receive_signal)

        self.id = os.getpid()
        logger.debug("Worker started: id={self.id} application={self.application_path} broker_class={self.broker.__class__.__name__}".format(self=self))

        import_module(self.application_path)

        #TODO
        # write to file pid for notifiing supervisor during start

        while self.running:
            task_pack = self.broker.pull_task(self.id)
            if not task_pack:
                sleep(0.1)
                continue

            task, task_args, task_kwargs = loads(task_pack)
            task_result = self.run_task(task, task_args, task_kwargs)
            self.broker.finish_task(self.id)
            logger.debug('task_result {}'.format(task_result))

        # TODO delete file pid