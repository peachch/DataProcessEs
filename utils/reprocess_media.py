import logging

from common.mongo_helper import MongoHelper
from settings import PENDING


def reprocess_album(vendor, channel=None, media_id=None, sys_time=None):
    """重新处理指定数据，修复bug或其它需要重跑数据时使用"""
    query_dict = {}
    query_dict.update({"vendor": vendor})
    if media_id is not None:
        query_dict.update({"id": media_id})
    if channel is not None:
        query_dict.update({"channel":channel})
    if sys_time is not None:
        query_dict.update({"sysTime": {"$gte": sys_time}})
    mongo_conn = MongoHelper()
    status = mongo_conn.update(coll="ori_album", dict_filter=query_dict,
                               dict_update={"$set": {"data_state": PENDING}})
    logging.info(f"success reprocessing media count {status.modified_count}")


def reprocess_video(vendor, channel=None, media_id=None, sys_time=None):
    """重新处理指定数据，修复bug或其它需要重跑数据时使用"""
    query_dict = {}
    query_dict.update({"vendor": vendor})
    if media_id is not None:
        query_dict.update({"id": media_id})
    if channel is not None:
        query_dict.update({"channel": channel})
    if sys_time is not None:
        query_dict.update({"sysTime": {"$gte": sys_time}})
    mongo_conn = MongoHelper()
    status = mongo_conn.update(coll="ori_video", dict_filter=query_dict,
                               dict_update={"$set": {"data_state": PENDING}})

    logging.info(f"success reprocessing media count {status.modified_count}")
