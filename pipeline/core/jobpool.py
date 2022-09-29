
import logging
import time
from collections import OrderedDict
from functools import partial
from threading import Thread, Lock
from common.mongo_helper import MediaMongoHelper


class Job(object):
    def __init__(self, media, coll):
        self.coll = coll
        self.media = media
        self.id = media["id"]

    # def end(self):
    #     pass


class JobPool(Thread):
    """从数据库获取待处理媒资"""

    def __init__(self, vendor, coll, max_workers, state_field="data_state"):
        self.vendor = vendor
        self.coll = coll
        self.logger = logging
        self.state_field = state_field
        self.mongo_conn = MediaMongoHelper(self.coll)
        self._pool = OrderedDict()
        self.lock = Lock()
        self.max_workers = max_workers
        Thread.__init__(self)

    def run(self) -> None:
        self._pool_keeper(max_workers=self.max_workers)

    def _pool_keeper(self, max_workers):
        """循环从数据库获取需要处理的数据"""
        max_workers = max(2, max_workers)
        threshold = max_workers * 20

        while True:
            time.sleep(0.1)
            if len(self._pool) > threshold // 15:
                continue

            medias = self.mongo_conn.get_next_process_media(vendor=self.vendor, max_len=threshold, state_field=self.state_field)
            if not medias:
                time.sleep(20)
                continue

            else:
                with self.lock:
                    for media in medias:
                        if media["id"] not in self._pool:
                            media.pop("_id")  # mongodb默认id
                            job = Job(media=media, coll=self.coll)
                            job.end = partial(self.job_done, media_id=media["id"])
                            self._pool[media["id"]] = job

    def job_done(self, media_id):
        self.mongo_conn.update_media_status(media_id, self.state_field)

    def job_error(self, media_id):
        """处理失败"""
        pass

    def fetch_job(self):
        """返回待处理数据生成器"""
        while True:
            if len(self._pool) > 0:
                with self.lock:
                    yield self._pool.popitem(last=False)[1]
            else:
                time.sleep(0.01)
