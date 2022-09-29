
import re
from datetime import datetime
import json
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED

import hashlib
import os
import signal
import sys
import time
from copy import copy
from retry import retry
from common.oss_helper import OssHelper

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.kafka_helper import KafkaHelper
from common.mongo_helper import MongoHelper
import settings
import logging

VERSION = 1.0  # 版本号


class SyncMediaHandler(object):
    """媒资同步"""
    kafka_bootstrap_servers = settings.BOOTSTRAP_SERVERS
    kafka_cursor_offset = "latest"
    oss_url = ""
    oss_key = ""
    oss_key_secret = ""
    oss_data_type = None  # 指定分集、专辑数据集
    kafka_group_id = None  # 指定分集、专辑分组ID
    kafka_topic = None  # 指定分集、专辑topic
    coll = None  # mongodb数据集
    media_type = None  # album/video

    def __init__(self):
        self.logger = logging
        self.mongo_conn = MongoHelper()

        # 安全退出
        self.is_sigint_up = False
        self.sigint_up_count = 0
        signal.signal(signal.SIGINT, self.sigint_handler)
        # signal.signal(signal.SIGHUP, sigint_handler)
        signal.signal(signal.SIGTERM, self.sigint_handler)

    def sigint_handler(self, signum, frame):
        self.is_sigint_up = True
        self.logger.warning('Catched interrupt signal!')
        self.sigint_up_count += 1
        if self.sigint_up_count >= 3:
            # 强制退出
            try:
                os._exit(0)
            except:
                self.logger.info('program is dead.')
            finally:
                self.logger.info('clean up')

    @staticmethod
    def validator_media(media: dict):
        """过滤无用分集数据"""
        raise NotImplementedError

    def filter_media(self, vendor: str, channel: str):
        """数据必须在预设的范围内"""
        assert self.media_type is not None, "data_type undefined"
        available = False
        if vendor in settings.AVAILABLE_DATA[self.media_type]:
            channel_domains = settings.AVAILABLE_DATA[self.media_type][vendor]
            assert isinstance(channel_domains, list), "settings params AVAILABLE_DATA error"
            if channel_domains == [] or channel in channel_domains:
                available = True
        return available

    @staticmethod
    def generate_media_entropy(media: dict):
        """通过hash值对比数据差异"""
        media_copy = copy(media)

        for field in ["data_source", "updateTime", "imp_date", "sysTime", "data_timestamp"]:
            if field in media_copy:
                media_copy.pop(field)  # 去除无关字段生成摘要

        ori_media_str = str(media_copy)
        data_digest = hashlib.md5(ori_media_str.encode()).hexdigest()
        del ori_media_str, media_copy
        return data_digest

    def origin_media_preprocess(self, media: dict):
        """原始媒资添加id、特征值等字段"""
        raise NotImplementedError

    def save_album_change_record(self, media_old, media_new):
        """记录媒资album上下架信息 20211028"""
        coll = "album_change_record"
        timestamp = int(time.time() * 1000)
        record_id = media_new["id"] + "_" + str(timestamp)      # 如果有写入异常可能导致重复创建
        album_id = media_new["id"]
        media_change_record = dict(record_id=record_id, timestamp=timestamp, album_id=album_id,
                                   status_old=media_old.get("status", 0), status_new=media_new["status"],
                                   media_old=media_old, media_new=media_new)
        status = self.mongo_conn.replace_one(coll=coll, dict_filter={"record_id": record_id},
                                             dict_replacement=media_change_record)
        self.logger.debug(f'{dict(record_id=record_id, status_old=media_old.get("status", 0), status_new=media_new["status"])}')

    @retry(tries=3)
    def create_or_update_media(self, media: dict):
        """创建或更新媒资"""
        # 好像逻辑有点乱，可以对比下git记录
        assert self.coll is not None, "mongodb collection undefined"
        sys_time = media["sysTime"]
        data_digest = media["data_digest"]
        data_id = media["id"]
        # 时间最新、数据有变化
        existed_data = self.mongo_conn.find_one(self.coll, {'id': data_id})
        media_old = {}
        if existed_data:
            media_old.update(existed_data)
            media_old.pop("_id")
            if existed_data["data_digest"] == data_digest or datetime.strptime(existed_data["sysTime"], '%Y-%m-%d %H:%M:%S') \
                    > datetime.strptime(sys_time, '%Y-%m-%d %H:%M:%S'):
                # 如果时间较前只更新sysTime字段, kafka大量更新实际上只更新了时间
                existed_data_systime = existed_data["sysTime"]
                if datetime.strptime(existed_data_systime, '%Y-%m-%d %H:%M:%S') < datetime.strptime(sys_time, '%Y-%m-%d %H:%M:%S'):
                    reset_field = {"data_timestamp": int(time.time() * 1000)}
                    for filed in ["data_source", "updateTime", "imp_date", "sysTime"]:
                        if filed in media:
                            reset_field[filed] = media[filed]  # 这些字段更新到最新
                    self.mongo_conn.update(coll=self.coll, dict_filter={"id": data_id},  dict_update={"$set": reset_field})
                self.logger.info(f"Old media: {media['id']} {media['title']} {sys_time}")
                return

            # oss数据有出错的可能，如果时间相同，不能用oss数据覆盖kafka数据！！
            if datetime.strptime(existed_data["sysTime"], '%Y-%m-%d %H:%M:%S') == datetime.strptime(sys_time, '%Y-%m-%d %H:%M:%S'):
                if "kafka" in existed_data["data_source"] and "oss" in media["data_source"]:
                    self.logger.info(f"Old media: {media['id']} {media['title']} {sys_time}")
                    return

        # 时间戳记录更新时间,覆盖数据
        media.update({"data_state": settings.PENDING, "data_timestamp": int(time.time() * 1000)})
        self.mongo_conn.replace_one(self.coll, {'id': data_id}, media, upsert=True)
        self.logger.info(f"New media: {media['id']} {media['title']}")

        # 记录专辑媒资上下架
        # if self.media_type == settings.ALBUM and str(media["status"]) != str(media_old.get("status", 0)):  # 如果上下架状态变化
        #     self.save_album_change_record(media_old=media_old, media_new=media)

    def process_media(self, media: dict):
        """媒资处理"""
        if self.filter_media(vendor=media["vendor"], channel=media["channel"]):

            if self.validator_media(media):
                origin_media = self.origin_media_preprocess(media)
                self.logger.info(f"Receive media: {origin_media['id']} {origin_media['title']}")
                self.create_or_update_media(media=origin_media)
            else:
                self.logger.warning(f"Filter media: {media}")

    @staticmethod
    def format_ext_field(ext_value):
        """ext 字段被重复转义"""
        if isinstance(ext_value, dict):
            return ext_value
        remove_escape_character = ext_value.replace("\\", "")
        ext_content_matched = re.search("\{(?P<ext_content>.+)\}", remove_escape_character)
        if not ext_content_matched:
            return dict()
        else:
            ext_content = ext_content_matched.group("ext_content")
            ext_dict = json.loads("{" + ext_content + "}")
            assert isinstance(ext_dict, dict)
            return ext_dict

    def _import_media_from_kafka(self):
        """从kafka增量更新媒资数据"""
        assert None not in [self.kafka_cursor_offset, self.kafka_topic, self.kafka_group_id], "kafka params error"
        kafka_helper = KafkaHelper(group_id=self.kafka_group_id, topic=self.kafka_topic,
                                   bootstrap_servers=self.kafka_bootstrap_servers,
                                   cursor_offset=self.kafka_cursor_offset)
        ori_media_iterator = kafka_helper.get_media_data()
        for media_dict in ori_media_iterator:
            media_dict.update({"data_source": f"kafka @v{VERSION}"})
            self.process_media(media=media_dict)
            if self.is_sigint_up:
                break
        kafka_helper.close()

    def _import_media_from_oss(self, date, query):
        """从oss导入指定日期全量数据,支持query筛选"""
        assert self.oss_data_type is not None, "oss_data_type undefined"
        oss_helper = OssHelper(url=self.oss_url, key=self.oss_key, key_secret=self.oss_key_secret,
                               data_type=self.oss_data_type)
        download_fields = oss_helper.start_download(date)
        if not download_fields:
            return
        self.logger.info(f"downloaded {len(download_fields)} files, filepath {os.path.join(oss_helper.path_dir, date)}")

        # 所有字段名  TODO oss和kafka数据有差异，可能会导致大量数据被更新，尽量使用query更新指定媒资，主要用于补充缺失数据或者冷启动
        target_field_names = {'hPicMd5', 'shortTitle', 'publishYear', 'data_timestamp', 'needPay', 'index', 'vPic',
                              'channel', 'fullPinyin', 'id', 'title', 'duration', 'tags', 'channelId', 'last',
                              'payMode', 'vid', 'english', 'aid', 'completed', 'type', 'regions', 'directors',
                              'updateTime', 'data_digest', 'simplePinyin', 'data_state', 'episode', 'languages',
                              'total', 'brief', 'params', 'vendor', 'score', 'producers', 'albumIds', 'status',
                              'enTitle', 'publishDate', 'douBanTags', 'ext', 'actors', 'exclusive', 'hPic', 'sysTime',
                              'vPicMd5', 'playUrl', 'aliases'}
        field_name_mapping = {n.lower(): n for n in target_field_names}
        media_iterator = oss_helper.read_parquet_date(date, query)

        executor = ThreadPoolExecutor(max_workers=12)
        all_tasks = []
        for ori_media in media_iterator:
            # oss字段名被处理成了小写,需要映射成原始大小写,尽量和kafka数据一致、 ext字段被处理成了json
            media_dict = {field_name_mapping.get(k, k): v for k, v in ori_media.items()}
            # 记录数据来源和脚本版本
            media_dict.update({"data_source": f"oss @v{VERSION}"})

            future = executor.submit(self.process_media, media=media_dict)
            all_tasks.append(future)
            if len(all_tasks) >= 4800:
                # 避免创建大量任务导致内存泄漏
                wait(all_tasks, return_when=ALL_COMPLETED)
                all_tasks = []
            if self.is_sigint_up:
                break
        executor.shutdown()

    def sync_from_kafka(self):
        """从kafka导入实时增量数据"""
        self.logger.info("*" * 24 + "\t媒资同步程序启动\t" + "*" * 24)
        self._import_media_from_kafka()
        self.logger.info("*" * 24 + "\t安全退出\t" + "*" * 24)

    def sync_from_oss(self, date, query=None):
        """从oss一次性导入所有媒资数据"""
        self.logger.info("*" * 24 + "\t媒资全量更新程序启动\t" + "*" * 24)
        self._import_media_from_oss(date=date, query=query)
        self.logger.info("*" * 24 + "\t安全退出\t" + "*" * 24)


class SyncAlbumHandler(SyncMediaHandler):
    """同步媒资 专辑数据"""

    def __init__(self, cursor_offset="latest"):
        self.coll = settings.ORI_ALBUM_COLL
        self.kafka_group_id = settings.ALBUM_GROUP_ID
        self.kafka_topic = settings.ALBUM_TOPIC
        self.kafka_cursor_offset = cursor_offset
        self.oss_data_type = "media_leiniao_album_partition_by_endtime"
        self.media_type = settings.ALBUM
        SyncMediaHandler.__init__(self)

    @staticmethod
    def validator_media(media):
        """过滤无用专辑数据"""
        checklist = [
            media.get("vendor", False),
            media.get("aid", False),
            media.get("title", False),
            media.get("sysTime", False),
        ]
        return all(checklist)

    def origin_media_preprocess(self, media):
        """原始媒资添加id、特征值等字段"""
        media["ext"] = self.format_ext_field(media.get("ext", {}))
        ori_album_digest = self.generate_media_entropy(media)
        media["data_digest"] = ori_album_digest
        data_id = media["vendor"] + media["aid"]  # 专辑唯一id
        media["id"] = data_id

        # 数组字段重新排序
        for value in media.values():
            if isinstance(value, list):
                value.sort()
        return media


class SyncVideoHandler(SyncMediaHandler):
    """同步媒资 分集数据"""

    def __init__(self, cursor_offset="latest"):
        self.coll = settings.ORI_VIDEO_COLL
        self.kafka_group_id = settings.VIDEO_GROUP_ID
        self.kafka_topic = settings.VIDEO_TOPIC
        self.kafka_cursor_offset = cursor_offset
        self.oss_data_type = "media_leiniao_video_partition_by_endtime"
        self.media_type = settings.VIDEO
        SyncMediaHandler.__init__(self)

    @staticmethod
    def validator_media(media):
        """过滤无用分集数据"""
        checklist = [
            media.get("vid", False),
            media.get("vendor", False),
            media.get("sysTime", False),
            # 必须有关联的aid
            len(media.get("albumIds", [])) >= 1,
            # media.get("title", False)     # B站大量分集无标题
        ]
        return all(checklist)

    def origin_media_preprocess(self, media):
        """原始媒资添加id、特征值等字段"""
        media["ext"] = self.format_ext_field(media.get("ext", {}))
        ori_video_digest = self.generate_media_entropy(media)
        media["data_digest"] = ori_video_digest
        data_id = media["vendor"] + media["vid"]  # 分集唯一id
        media["id"] = data_id

        # 数组字段重新排序
        for value in media.values():
            if isinstance(value, list):
                value.sort()
        return media
