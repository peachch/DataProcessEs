
import logging
from threading import Lock
from pymongo import MongoClient
from retry import retry

import settings
from utils.utils_class import singleton


@singleton
class MongoHelper(object):

    def __init__(self, host=settings.MONGODB_HOST, port=settings.MONGODB_PORT,
                 db=settings.MONGODB_MEDIA_DB):
        self._client = MongoClient(host=host, port=port)
        self._db = self._client[db]
        self._client.server_info()
        self._db.command('ping')
        logging.info(f"MongoDB {host}:{port} connect success!")

    @property
    def client(self):
        return self._client

    @property
    def db(self):
        return self._db

    def bulk_write(self, coll, actions):
        result = self._db[coll].bulk_write(requests=actions)
        return result

    def replace_one(self, coll, dict_filter: dict, dict_replacement: dict, upsert=True):
        result = self._db[coll].replace_one(dict_filter, dict_replacement, upsert=upsert)
        return result

    def find_one(self, coll, dict_filter):
        result = self._db[coll].find_one(dict_filter)
        return result

    def find(self, coll: str, dict_filter=None, limit_num=0):
        # todo 增加排序
        if dict_filter is None:
            dict_filter = {}
        result = self._db[coll].find(dict_filter, batch_size=1000).limit(limit_num)
        if result.count() == 0:
            return []
        return result

    def update(self, coll, dict_filter: dict, dict_update: dict):
        result = self._db[coll].update_many(dict_filter, dict_update)  # {"$set":{}}
        return result

    def delete_many(self, coll, dict_filter: dict):
        result = self._db[coll].delete_many(dict_filter)
        return result

    def is_connected(self):
        try:
            self.client.db_name.command("ping")
        except Exception:
            return False
        else:
            return True

    def get_coll_names(self):
        coll_names = self._db.list_collection_names(session=None)
        return coll_names


class MediaMongoHelper(object):
    def __init__(self, coll):
        """原始媒资库相关"""
        self.coll = coll
        self.processing_media = dict()
        self.lock = Lock()
        self.logger = logging
        self.mongo_conn = MongoHelper()  # 单例无法继承

    def get_next_process_media(self, vendor, state_field="data_state", max_len=100):
        # 未处理数据,一次最多取100条
        # TODO 优化线程安全问题
        cursor = self.mongo_conn._db[self.coll].find({state_field: settings.PENDING, "vendor": vendor}).sort('data_timestamp', 1).limit(max_len)
        media_next_process = []
        for media in cursor:
            data_id = media["id"]
            with self.lock:
                # 记录正在处理的数据，防止多线程重复取数问题
                if data_id not in self.processing_media:
                    self.processing_media[data_id] = "processing"

                    media_next_process.append(media)
        return media_next_process

    @retry(tries=3)
    def update_media_status(self, media_id, state_field="data_state", status=settings.SUCCEED):
        """处理完的数据更新数据状态"""
        self.mongo_conn.update(self.coll, {"id": media_id}, {"$set": {state_field: status}})

        with self.lock:
            self.processing_media.pop(media_id)
