
import json
import logging
import time
from typing import Iterable
from kafka import KafkaConsumer


class KafkaHelper(object):
    def __init__(self, group_id, topic, bootstrap_servers, cursor_offset="latest"):
        # 连接单个topic
        self.group_id = group_id
        self.consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers,
                                      group_id=group_id,
                                      auto_offset_reset=cursor_offset,
                                      enable_auto_commit=True,  # 自动提交消费数据的offset
                                      auto_commit_interval_ms=1000,  # 自动提交频率，减少异常重启后数据丢失
                                      # consumer_timeout_ms=10000,  # 如果1秒内kafka中没有可供消费的数据，自动退出
                                      )

        self.consumer.subscribe(topic)
        self.logger = logging
        self.logger.warning("Kafka connect success!")

    def get_media_data(self) -> Iterable:
        # 返回一个生成器
        for message in self.consumer:

            if message is not None:
                media_dict = json.loads(message.value.decode())
                if not isinstance(media_dict, dict):
                    logging.error(f"message value must be dict objects, got {media_dict}")
                    continue

                self.logger.info(f"topic:{message.topic},partition:{message.partition},offset:{message.offset},group_id:{self.group_id},"
                                 f"sys_time:{media_dict.get('sysTime','')},"
                                 f"vendor:{media_dict.get('vendor','')},"
                                 f"channel:{media_dict.get('channel','')},title:{media_dict.get('title','')}")
                yield media_dict

    def close(self):
        """关闭时提交偏移量"""
        time.sleep(2)
        self.consumer.close()


