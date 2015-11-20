from __future__ import absolute_import

import datetime
import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from frontera import Backend, Metadata, States, Queue
from frontera.utils.misc import load_object, get_crc32
from frontera.utils.url import parse_domain_from_url_fast
from frontera.core.models import Request, Response

from frontera.contrib.backends import Crc32NamePartitioner
from frontera.contrib.backends.sqlalchemy.models import MetadataModel, StateModel, QueueModel, DeclarativeBase

# Default settings
DEFAULT_ENGINE = 'sqlite:///:memory:'
DEFAULT_ENGINE_ECHO = False
DEFAULT_DROP_ALL_TABLES = True
DEFAULT_CLEAR_CONTENT = True
DEFAULT_MODELS = {
    'MetadataModel': 'frontera.contrib.backends.sqlalchemy.models.MetadataModel',
}


class SQLAlchemyMetadata(Metadata):
    def __init__(self, session_cls, model_cls):
        self.session = session_cls(expire_on_commit=False)   # FIXME: Should be explicitly mentioned in docs
        self.metadata_model = model_cls
        self.table = DeclarativeBase.metadata.tables['metadata']

    def add_seeds(self, seeds):
        self.session.add_all(map(self._create_page, seeds))
        self.session.commit()

    def request_error(self, page, error):
        m = self._create_page(page)
        m.error = error
        self.session.merge(m)
        self.session.commit()

    def page_crawled(self, response, links):
        self.session.merge(self._create_page(response))
        for link in links:
            self.session.merge(self._create_page(link))
        self.session.commit()

    def _create_page(self, obj):
        db_page = self.metadata_model()
        db_page.fingerprint = obj.meta['fingerprint']
        db_page.url = obj.url
        db_page.created_at = datetime.datetime.utcnow()
        db_page.meta = obj.meta
        db_page.depth = 0

        if isinstance(obj, Request):
            db_page.headers = obj.headers
            db_page.method = obj.method
            db_page.cookies = obj.cookies
        elif isinstance(obj, Response):
            db_page.headers = obj.request.headers
            db_page.method = obj.request.method
            db_page.cookies = obj.request.cookies
            db_page.status_code = obj.status_code
        return db_page


class SQLAlchemyState(States):
    def __init__(self, session_cls, model_cls, cache_size_limit):
        self.session = session_cls()
        self.model = model_cls
        self.table = DeclarativeBase.metadata.tables['states']
        self._cache = dict()
        self.logger = logging.getLogger("frontera.contrib.backends.sqlalchemy.SQLAlchemyState")
        self._cache_size_limit = cache_size_limit

    def fetch(self, fingerprints):
        to_fetch = [f for f in fingerprints if f not in self._cache]
        self.logger.debug("cache size %s" % len(self._cache))
        self.logger.debug("to fetch %d from %d" % (len(to_fetch), len(fingerprints)))

        for state in self.session.query(self.model).filter(self.model.fingerprint.in_(to_fetch)):
            self._cache[state.fingerprint] = state.state

    def flush(self, force_clear):
        if len(self._cache) > self._cache_size_limit:
            force_clear = True
        for fingerprint, state_val in self._cache.iteritems():
            state = self.model(fingerprint=fingerprint, state=state_val)
            self.session.add(state)
        self.session.commit()
        if force_clear:
            self.logger.debug("Cache has %d items, clearing" % len(self._cache))
            self._cache.clear()

    def _put(self, obj):
        if obj.meta['state'] is not None:
            self._cache[obj.meta['fingerprint']] = obj.meta['state']

    def _get(self, obj):
        fprint = obj.meta['fingerprint']
        obj.meta['state'] = self._cache[fprint] if fprint in self._cache else None

    def update_cache(self, objs):
        objs = objs if type(objs) in [list, tuple] else [objs]
        map(self._put, objs)

    def set_states(self, objs):
        objs = objs if type(objs) in [list, tuple] else [objs]
        map(self._get, objs)


class SQLAlchemyQueue(Queue):

    GET_RETRIES = 3

    def __init__(self, session_cls, model_cls, metadata_model_cls, partitions):
        self.session = session_cls()
        self.queue_model = model_cls
        self.metadata_model = metadata_model_cls
        self.table = DeclarativeBase.metadata.tables['queue']
        self.logger = logging.getLogger("frontera.contrib.backends.sqlalchemy.SQLAlchemyQueue")
        self.partitions = [i for i in range(0, partitions)]
        self.partitioner = Crc32NamePartitioner(self.partitions)

    def get_next_requests(self, max_n_requests, partition_id, **kwargs):
        """
        Dequeues new batch of requests for crawling.

        Priorities, from highest to lowest:
         - max_requests_per_host
         - max_n_requests
         - min_hosts & min_requests

        :param max_n_requests:
        :param partition_id:
        :param kwargs: min_requests, min_hosts, max_requests_per_host
        :return: list of :class:`Request <frontera.core.models.Request>` objects.
        """
        min_requests = kwargs.pop("min_requests")
        min_hosts = kwargs.pop("min_hosts")
        max_requests_per_host = kwargs.pop("max_requests_per_host")
        assert(max_n_requests > min_requests)

        queue = {}
        limit = min_requests
        tries = 0
        count = 0
        while tries < self.GET_RETRIES:
            tries += 1
            limit *= 5.5 if tries > 1 else 1.0
            self.logger.debug("Try %d, limit %d, last attempt: requests %d, hosts %d" %
                              (tries, limit, count, len(queue.keys())))
            queue.clear()
            count = 0
            for item in self.session.query(self.queue_model).filter_by(partition_id=partition_id).\
                                                             order_by(self.queue_model.score).limit(limit):
                if item.host_crc32 not in queue:
                    queue[item.host_crc32] = []
                if max_requests_per_host is not None and len(queue[item.host_crc32]) > max_requests_per_host:
                    continue
                queue[item.host_crc32].append(item)
                count += 1
                if count > max_n_requests:
                    break

            if min_hosts is not None and len(queue.keys()) < min_hosts:
                continue

            if count < min_requests:
                continue
            break
        self.logger.debug("Finished: tries %d, hosts %d, requests %d" % (tries, len(queue.keys()), count))

        results = []
        for item in queue.itervalues():
            method = 'GET' if not item.method else item.method
            results.append(Request(item.url, method=method, meta=item.meta, headers=item.headers, cookies=item.cookies))
            item.delete()
        self.session.commit()
        return results

    def schedule(self, batch):
        for fprint, (score, url, schedule) in batch.iteritems():
            if schedule:
                _, hostname, _, _, _, _ = parse_domain_from_url_fast(url)
                if not hostname:
                    self.logger.error("Can't get hostname for URL %s, fingerprint %s" % (url, fprint))
                    continue
                partition_id = self.partitioner.partition(hostname, self.partitions)
                host_crc32 = get_crc32(hostname)
                q = self.queue_model(fingerprint=fprint, score=score, url=url, partition_id=partition_id,
                                     host_crc32=host_crc32)
                self.session.add(q)
            m = self.metadata_model(fingerprint=fprint, score=score)
            self.session.merge(m)
        self.session.commit()

    def count(self):
        return self.session.query(self.queue_model).count()


class SQLAlchemyBackend(Backend):
    component_name = 'SQLAlchemy Backend'

    def __init__(self, manager):
        self.manager = manager

        # Get settings
        settings = manager.settings
        engine = settings.get('SQLALCHEMYBACKEND_ENGINE', DEFAULT_ENGINE)
        engine_echo = settings.get('SQLALCHEMYBACKEND_ENGINE_ECHO', DEFAULT_ENGINE_ECHO)
        drop_all_tables = settings.get('SQLALCHEMYBACKEND_DROP_ALL_TABLES', DEFAULT_DROP_ALL_TABLES)
        clear_content = settings.get('SQLALCHEMYBACKEND_CLEAR_CONTENT', DEFAULT_CLEAR_CONTENT)
        models = settings.get('SQLALCHEMYBACKEND_MODELS', DEFAULT_MODELS)

        # Create engine
        self.engine = create_engine(engine, echo=engine_echo)

        # Load models
        self.models = dict([(name, load_object(klass)) for name, klass in models.items()])

        # Drop tables if we have to
        if drop_all_tables:
            DeclarativeBase.metadata.drop_all(self.engine)
        DeclarativeBase.metadata.create_all(self.engine)

        # Create session
        Session = sessionmaker()
        Session.configure(bind=self.engine)
        self.session = Session()

        # Clear content if we have to
        if clear_content:
            for name, table in DeclarativeBase.metadata.tables.items():
                self.session.execute(table.delete())

    @classmethod
    def from_manager(cls, manager):
        return cls(manager)

    @property
    def page_model(self):
        return self.models['MetadataModel']

    def frontier_start(self):
        pass

    def frontier_stop(self):
        self.session.close()
        self.engine.dispose()

    def add_seeds(self, seeds):
        for seed in seeds:
            db_page, _ = self._get_or_create_db_page(seed)
        self.session.commit()

    def get_next_requests(self, max_next_requests, **kwargs):
        query = self.page_model.query(self.session)
        query = query.filter(self.page_model.state == MetadataModel.State.NOT_CRAWLED)
        query = self._get_order_by(query)
        if max_next_requests:
            query = query.limit(max_next_requests)
        next_pages = []
        for db_page in query:
            db_page.state = MetadataModel.State.QUEUED
            request = self.manager.request_model(url=db_page.url, meta=db_page.meta, headers=db_page.headers,
                                                 cookies=db_page.cookies, method=db_page.method)
            next_pages.append(request)
        self.session.commit()
        return next_pages

    def page_crawled(self, response, links):
        db_page, _ = self._get_or_create_db_page(response)
        db_page.state = MetadataModel.State.CRAWLED
        db_page.status_code = response.status_code
        for link in links:
            db_page_from_link, created = self._get_or_create_db_page(link)
            if created:
                db_page_from_link.depth = db_page.depth+1
        self.session.commit()

    def request_error(self, request, error):
        db_page, _ = self._get_or_create_db_page(request)
        db_page.state = MetadataModel.State.ERROR
        db_page.error = error
        self.session.commit()

    def _get_or_create_db_page(self, obj):
        if not self._request_exists(obj.meta['fingerprint']):
            db_page = self._create_page(obj)
            self.session.add(db_page)
            self.manager.logger.backend.debug('Creating request %s' % db_page)
            return db_page, True
        else:
            db_page = self.page_model.query(self.session).filter_by(fingerprint=obj.meta['fingerprint']).first()
            self.manager.logger.backend.debug('Request exists %s' % db_page)
            return db_page, False

    def _request_exists(self, fingerprint):
        q = self.page_model.query(self.session).filter_by(fingerprint=fingerprint)
        return self.session.query(q.exists()).scalar()

    def _get_order_by(self, query):
        raise NotImplementedError

    def _create_page(self, obj):
        db_page = self.page_model()
        db_page.fingerprint = obj.meta['fingerprint']
        db_page.state = MetadataModel.State.NOT_CRAWLED
        db_page.url = obj.url
        db_page.created_at = datetime.datetime.utcnow()
        db_page.meta = obj.meta
        db_page.depth = 0

        if isinstance(obj, Request):
            db_page.headers = obj.headers
            db_page.method = obj.method
            db_page.cookies = obj.cookies
        elif isinstance(obj, Response):
            db_page.headers = obj.request.headers
            db_page.method = obj.request.method
            db_page.cookies = obj.request.cookies
        return db_page


class FIFOBackend(SQLAlchemyBackend):
    component_name = 'SQLAlchemy FIFO Backend'

    def _get_order_by(self, query):
        return query.order_by(self.page_model.created_at)


class LIFOBackend(SQLAlchemyBackend):
    component_name = 'SQLAlchemy LIFO Backend'

    def _get_order_by(self, query):
        return query.order_by(self.page_model.created_at.desc())


class DFSBackend(SQLAlchemyBackend):
    component_name = 'SQLAlchemy DFS Backend'

    def _get_order_by(self, query):
        return query.order_by(self.page_model.depth.desc(), self.page_model.created_at)


class BFSBackend(SQLAlchemyBackend):
    component_name = 'SQLAlchemy BFS Backend'

    def _get_order_by(self, query):
        return query.order_by(self.page_model.depth, self.page_model.created_at)


BASE = SQLAlchemyBackend
LIFO = LIFOBackend
FIFO = FIFOBackend
DFS = DFSBackend
BFS = BFSBackend