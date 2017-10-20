# from retasks.brokers import Broker
# try:
#     from sqlalchemy import create_engine, MetaData
#     from sqlalchemy.orm import sessionmaker, scoped_session
#     from sqlalchemy.pool import NullPool
#     from sqlalchemy.ext.declarative import declarative_base
#     from sqlalchemy import Column, Integer, String, DateTime
# except ImportError:
#     raise ImportError("SQLAlchemy not found. Install 'sqlalchemy' package.")
#
#
# Base = declarative_base()
#
#
# class Queue(Base):
#     __tablename__ = 'task_queue'
#
#     scheduled = Column(DateTime, default=)
#     task_id = Column(String)
#
#
# class SqlBroker(Broker):
#     def __init__(self, connection_string, **kwargs):
#         engine = create_engine(
#             connection_string,
#             connect_args=kwargs,
#             poolclass=NullPool  # DON'T CHANGE - any pooling causes random fails if connection is not reliable
#         )
#
#         metadata = MetaData(engine)
#
#         session_factory = sessionmaker(bind=engine)
#
#         Session = scoped_session(session_factory)
#
#     def new_task(self, task_id, task_pack):
#         task_key = self._get_task_key(task_id)
#
#         if self._get_task_pack_by_task_key(task_key):
#             return
#
#         with self._connection.pipeline() as pipeline:
#             pipeline.set(task_key, task_pack)
#             pipeline.lpush('task_queue', task_key)
#             pipeline.execute()
#
#     def pull_task(self, worker_id):
#         worker_key, task_key = self._get_keys_by_worker_id(worker_id)
#
#         if task_key:
#             return self._get_task_pack_by_task_key(task_key)
#
#         task_key = self._connection.rpop('task_queue')
#
#         if not task_key:
#             return None
#
#         self._connection.set(worker_key, task_key)
#
#         return self._get_task_pack_by_task_key(task_key)
#
#     def finish_task(self, worker_id):
#         worker_key, task_key = self._get_keys_by_worker_id(worker_id)
#
#         self._connection.delete(worker_key)
#         self._connection.delete(task_key)
#
#     def terminate_worker(self, worker_id):
#         worker_key, task_key = self._get_keys_by_worker_id(worker_id)
#
#         if task_key:
#             self._connection.rpush(task_key)
#
