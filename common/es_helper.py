
import traceback
from concurrent.futures._base import wait
from concurrent.futures.thread import ThreadPoolExecutor
from functools import partial
from threading import Lock
import logging
import elasticsearch
from elasticsearch import RequestError
import elasticsearch.helpers
from retry import retry

lock = Lock()


def singleton_es(cls):
    """相同参数多线程下的 “单例” """
    _instance = {}

    def wrapper(server_url):
        key = str(server_url)
        with lock:  # 防止多线程同时执行实例化
            if not _instance.get(key):
                _instance[key] = cls(server_url)
        return _instance[key]

    return wrapper


@singleton_es
class ElasticHelper(object):

    def __init__(self, server_url: str):
        self._url = server_url
        self._connection = elasticsearch.Elasticsearch(self._url, timeout=1000)
        assert self._connection.ping()
        logging.info(f"Elasticsearch {server_url} connect success!")

    def __str__(self):
        return str(self._url)

    def refresh_index(self, index_name):
        self._connection.indices.refresh(index=index_name)

    def create_index(self, index_name, configuration=None):
        is_success = True
        try:
            self._connection.indices.create(index=index_name, body=configuration)
        except RequestError as e:
            if e.error == "resource_already_exists_exception":
                pass
            else:
                is_success = False
        finally:
            return is_success

    def delete_index(self, index_name):
        if self._connection.indices.exists(index_name):
            self._connection.indices.delete(index_name)
            logging.info(f'delete index: {index_name}')
        else:
            logging.error(f'index {index_name}  not found, skip delete this index')

    def update_index_bulk(self, index_name, sources):
        bulks = []
        for source in sources:  # sources = array of source dictionary
            bulks.append({"_index": index_name, "_source": source})
        status = elasticsearch.helpers.bulk(self._connection, bulks)
        return status

    @retry(tries=3)
    def bulk(self, bulk_actions):
        """ 执行批量操作"""
        if bulk_actions:
            status = elasticsearch.helpers.bulk(self._connection, bulk_actions, stats_only=True)
            return status

    @retry(tries=3)
    def update_document(self, index_name, document_id, source):
        elasticsearch.Elasticsearch.index(self._connection, index=index_name, id=document_id, body=source,
                                          doc_type="_doc")

    @retry(tries=3)
    def search(self, index_name, query):
        return self._connection.search(index=index_name, body=query)

    def get_source(self, index_name, doc_id):
        return self._connection.get_source(index=index_name, id=doc_id)

    @retry(tries=3)
    def delete_by_query(self, index_name, query):
        return self._connection.delete_by_query(index=index_name, body=query)


class MultiElasticHelper(object):  # todo 实现单例
    def __init__(self, multi_servers: list, max_workers=2):
        """同时将数据更新到多个ES数据库"""
        assert isinstance(multi_servers, list)
        self.servers = list(set(multi_servers))
        self.__executor = ThreadPoolExecutor(max_workers=max(len(self.servers) * 2, max_workers))  # 限制ES的访问频率 todo 性能瓶颈,可能阻塞
        self.instances = [ElasticHelper(server_url=url) for url in self.servers]

    def dispatcher(func):
        """同时操作多个ES数据库"""

        def wrapper(self, **kwargs):
            doing = list()
            result = list()

            def on_done(future, ins):
                e = future.exception()
                if e:
                    logging.error(f"es_server: {str(ins)} track_back: {''.join(traceback.format_tb(e.__traceback__))}")
                else:
                    logging.debug(f"es_server: {str(ins)}, action: {func.__name__}, result: {str(future.result())[:20]}")
                with lock:
                    result.append({"es_server": str(ins), "action": func.__name__, "result": future.result()})

            for instance in self.instances:
                f = self.__executor.submit(getattr(instance, func.__name__), **kwargs)
                f.add_done_callback(partial(on_done, ins=instance))
                doing.append(f)
            wait(doing)  # 同步操作，保证数据一致性，效率低，慎用！！！
            return result
        return wrapper

    @dispatcher
    def refresh_index(self, index_name):
        pass

    @dispatcher
    def create_index(self, index_name, configuration):
        pass

    @dispatcher
    def delete_index(self, index_name):
        pass

    @dispatcher
    def update_index_bulk(self, index_name, sources):
        pass

    @dispatcher
    def bulk(self, bulk_actions):
        pass

    @dispatcher
    def update_document(self, index_name, document_id, source):
        pass

    @dispatcher
    def search(self, index_name, query):
        pass

    @dispatcher
    def get_source(self, index_name, doc_id):
        pass

    @dispatcher
    def delete_by_query(self, index_name, query):
        pass
