import os
import re
import signal
import requests
import datetime
from collections import defaultdict
import json
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.kafka_helper import KafkaHelper
import settings
import logging
from common.es_helper import ElasticHelper


class SyncKafkaUserLog:
    """用户搜索日志kafka获取"""

    def __init__(self, cursor_offset="latest", es_service_host=settings.ELK_ES,
                 index_name=settings.INDEX_USER_SEARCH_LOG):
        self.logger = logging
        self.group_id = settings.USER_LOG_GROUP_ID
        self.topic = settings.USER_LOG_TOPIC
        self.bootstrap_servers = settings.BOOTSTRAP_SERVERS
        self.cursor_offset = cursor_offset
        self.index_name = index_name
        self.es_service_host = es_service_host

        # 记录已经创建过索引的es
        self.es_created_index = defaultdict(lambda: False)

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
                print('Program is dead.')
            finally:
                print('clean-up')

    def create_index(self, es_service_host):
        """创建索引"""
        # TODO 需要接入米兔数据，索引结构需要更新
        es_conn = ElasticHelper(es_service_host)

        if not self.es_created_index[es_conn.__str__()]:
            configuration = {
                "mappings": {
                    # "_doc": {
                    "properties": {
                        "log_timestamp": {
                            "type": "date"
                        },
                        "costTime": {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                    "ignore_above": 256,
                                    "type": "keyword"
                                }
                            }
                        },
                        "esSearchTime": {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                    "ignore_above": 256,
                                    "type": "keyword"
                                }
                            }
                        },
                        "host": {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                    "ignore_above": 256,
                                    "type": "keyword"
                                }
                            }
                        },
                        "message": {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                    "ignore_above": 256,
                                    "type": "keyword"
                                }
                            }
                        },
                        "previouslyMovie": {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                    "ignore_above": 256,
                                    "type": "keyword"
                                }
                            }
                        },
                        "requestJson": {
                            "properties": {
                                "context": {
                                    "properties": {
                                        "system": {
                                            "properties": {
                                                "application": {
                                                    "properties": {
                                                        "applicationId": {
                                                            "type": "text",
                                                            "fields": {
                                                                "keyword": {
                                                                    "ignore_above": 256,
                                                                    "type": "keyword"
                                                                }
                                                            }
                                                        }
                                                    }
                                                },
                                                "callbackurl": {
                                                    "type": "text",
                                                    "fields": {
                                                        "keyword": {
                                                            "ignore_above": 256,
                                                            "type": "keyword"
                                                        }
                                                    }
                                                },
                                                "device": {
                                                    "properties": {
                                                        "clientType": {
                                                            "type": "text",
                                                            "fields": {
                                                                "keyword": {
                                                                    "ignore_above": 256,
                                                                    "type": "keyword"
                                                                }
                                                            }
                                                        },
                                                        "deviceId": {
                                                            "type": "text",
                                                            "fields": {
                                                                "keyword": {
                                                                    "ignore_above": 256,
                                                                    "type": "keyword"
                                                                }
                                                            }
                                                        },
                                                        "mode": {
                                                            "type": "text",
                                                            "fields": {
                                                                "keyword": {
                                                                    "ignore_above": 256,
                                                                    "type": "keyword"
                                                                }
                                                            }
                                                        },
                                                        "scene": {
                                                            "type": "text",
                                                            "fields": {
                                                                "keyword": {
                                                                    "ignore_above": 256,
                                                                    "type": "keyword"
                                                                }
                                                            }
                                                        },
                                                        "sdkVersion": {
                                                            "type": "text",
                                                            "fields": {
                                                                "keyword": {
                                                                    "ignore_above": 256,
                                                                    "type": "keyword"
                                                                }
                                                            }
                                                        },
                                                        "supportedInterfaces": {
                                                            "properties": {
                                                                "audioPlayer": {
                                                                    "properties": {
                                                                        "clearQueue": {
                                                                            "type": "boolean"
                                                                        },
                                                                        "play": {
                                                                            "type": "boolean"
                                                                        },
                                                                        "stop": {
                                                                            "type": "boolean"
                                                                        }
                                                                    }
                                                                },
                                                            }
                                                        },
                                                        "topActivity": {
                                                            "type": "text",
                                                            "fields": {
                                                                "keyword": {
                                                                    "ignore_above": 256,
                                                                    "type": "keyword"
                                                                }
                                                            }
                                                        },
                                                        "user": {
                                                            "properties": {
                                                                "accessToken": {
                                                                    "type": "text",
                                                                    "fields": {
                                                                        "keyword": {
                                                                            "ignore_above": 256,
                                                                            "type": "keyword"
                                                                        }
                                                                    }
                                                                },
                                                                "latitude": {
                                                                    "type": "text",
                                                                    "fields": {
                                                                        "keyword": {
                                                                            "ignore_above": 256,
                                                                            "type": "keyword"
                                                                        }
                                                                    }
                                                                },
                                                                "longitude": {
                                                                    "type": "text",
                                                                    "fields": {
                                                                        "keyword": {
                                                                            "ignore_above": 256,
                                                                            "type": "keyword"
                                                                        }
                                                                    }
                                                                },
                                                                "queryid": {
                                                                    "type": "text",
                                                                    "fields": {
                                                                        "keyword": {
                                                                            "ignore_above": 256,
                                                                            "type": "keyword"
                                                                        }
                                                                    }
                                                                },
                                                                "userId": {
                                                                    "type": "text",
                                                                    "fields": {
                                                                        "keyword": {
                                                                            "ignore_above": 256,
                                                                            "type": "keyword"
                                                                        }
                                                                    }
                                                                },
                                                                "video": {
                                                                    "properties": {
                                                                        "dnum": {
                                                                            "type": "text",
                                                                            "fields": {
                                                                                "keyword": {
                                                                                    "ignore_above": 256,
                                                                                    "type": "keyword"
                                                                                }
                                                                            }
                                                                        },
                                                                        "mitu": {
                                                                            "properties": {
                                                                                "vip": {
                                                                                    "type": "boolean"
                                                                                }
                                                                            }
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                        },
                                                        "ip": {
                                                            "type": "text",
                                                            "fields": {
                                                                "keyword": {
                                                                    "ignore_above": 256,
                                                                    "type": "keyword"
                                                                }
                                                            }
                                                        }
                                                    }
                                                },
                                                "simple": {
                                                    "type": "boolean"
                                                },
                                                "iot": {
                                                    "properties": {
                                                        "callback":
                                                            {"type": "text",
                                                             "fields": {
                                                                 "keyword": {
                                                                     "ignore_above": 256,
                                                                     "type": "keyword"
                                                                 }
                                                             }},
                                                        "msg_receiver":
                                                            {"type": "text",
                                                             "fields": {
                                                                 "keyword": {
                                                                     "ignore_above": 256,
                                                                     "type": "keyword"
                                                                 }
                                                             }},
                                                        "o_token":
                                                            {"type": "text",
                                                             "fields": {
                                                                 "keyword": {
                                                                     "ignore_above": 256,
                                                                     "type": "keyword"
                                                                 }
                                                             }},
                                                    }
                                                },

                                            }
                                        }
                                    }
                                },
                                "request": {
                                    "properties": {
                                        "dialogState": {
                                            "type": "text",
                                            "fields": {
                                                "keyword": {
                                                    "ignore_above": 256,
                                                    "type": "keyword"
                                                }
                                            }
                                        },
                                        "key": {
                                            "type": "text",
                                            "fields": {
                                                "keyword": {
                                                    "ignore_above": 256,
                                                    "type": "keyword"
                                                }
                                            }
                                        },
                                        "nlu": {
                                            "properties": {
                                                "domain": {
                                                    "type": "text",
                                                    "fields": {
                                                        "keyword": {
                                                            "ignore_above": 256,
                                                            "type": "keyword"
                                                        }
                                                    }
                                                },
                                                "intent": {
                                                    "type": "text",
                                                    "fields": {
                                                        "keyword": {
                                                            "ignore_above": 256,
                                                            "type": "keyword"
                                                        }
                                                    }
                                                },
                                                "skill": {
                                                    "type": "text",
                                                    "fields": {
                                                        "keyword": {
                                                            "ignore_above": 256,
                                                            "type": "keyword"
                                                        }
                                                    }
                                                },
                                                "slots": {

                                                    "properties": {
                                                        "name": {
                                                            "type": "text",
                                                            "fields": {
                                                                "keyword": {
                                                                    "ignore_above": 256,
                                                                    "type": "keyword"
                                                                }
                                                            }
                                                        },

                                                        "value": {
                                                            "type": "text",
                                                            "fields": {
                                                                "keyword": {
                                                                    "ignore_above": 256,
                                                                    "type": "keyword"
                                                                }
                                                            }
                                                        }
                                                    }
                                                },
                                                "slots_nested": {
                                                    "type": "nested",
                                                    "properties": {
                                                        "name": {
                                                            "type": "text",
                                                            "fields": {
                                                                "keyword": {
                                                                    "ignore_above": 256,
                                                                    "type": "keyword"
                                                                }
                                                            }
                                                        },

                                                        "value": {
                                                            "type": "text",
                                                            "fields": {
                                                                "keyword": {
                                                                    "ignore_above": 256,
                                                                    "type": "keyword"
                                                                }
                                                            }
                                                        }
                                                    }
                                                },
                                                "queryText": {
                                                    "type": "text",
                                                    "fields": {
                                                        "keyword": {
                                                            "ignore_above": 256,
                                                            "type": "keyword"
                                                        }
                                                    }
                                                },
                                                "requestId": {
                                                    "type": "text",
                                                    "fields": {
                                                        "keyword": {
                                                            "ignore_above": 256,
                                                            "type": "keyword"
                                                        }
                                                    }
                                                },
                                                "timestamp": {
                                                    "type": "text",
                                                    "fields": {
                                                        "keyword": {
                                                            "ignore_above": 256,
                                                            "type": "keyword"
                                                        }
                                                    }
                                                },
                                                "tokens": {
                                                    "type": "text",
                                                    "fields": {
                                                        "keyword": {
                                                            "ignore_above": 256,
                                                            "type": "keyword"
                                                        }
                                                    }
                                                },
                                                "type": {
                                                    "type": "text",
                                                    "fields": {
                                                        "keyword": {
                                                            "ignore_above": 256,
                                                            "type": "keyword"
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                },
                                "session": {
                                    "properties": {
                                        "new": {
                                            "type": "boolean"
                                        },
                                        "sessionId": {
                                            "type": "text",
                                            "fields": {
                                                "keyword": {
                                                    "ignore_above": 256,
                                                    "type": "keyword"
                                                }
                                            }
                                        }
                                    }
                                },
                                "version": {
                                    "type": "text",
                                    "fields": {
                                        "keyword": {
                                            "ignore_above": 256,
                                            "type": "keyword"
                                        }
                                    }
                                }
                            }
                        },
                        "resources": {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                    "ignore_above": 256,
                                    "type": "keyword"
                                }
                            }
                        },
                        "searchPath": {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                    "ignore_above": 256,
                                    "type": "keyword"
                                }
                            }
                        },
                        "shortVideo": {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                    "ignore_above": 256,
                                    "type": "keyword"
                                }
                            }
                        },
                        "resultCode": {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                    "ignore_above": 256,
                                    "type": "keyword"
                                }
                            }
                        },
                        "tts": {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                    "ignore_above": 256,
                                    "type": "keyword"
                                }
                            }
                        },
                        "location": {"type": "geo_point"},
                    }
                }}
            es_conn.create_index(index_name=self.index_name, configuration=configuration)
            self.es_created_index[es_conn.__str__()] = True

    def _import_user_log_kafka2es(self):
        """从kafka获取用户日志数据"""
        kafka_conn = KafkaHelper(group_id=self.group_id, topic=self.topic,
                                 bootstrap_servers=self.bootstrap_servers, cursor_offset=self.cursor_offset)

        # host = "http://{}:{}/{}/_doc".format(self.publish_env[0]["host"], self.publish_env[0]["port"],
        #                                      settings.INDEX_USER_SEARCH_LOG)
        # es_conn = get_es_conn(self.publish_env)
        es_conn = ElasticHelper(server_url=self.es_service_host)

        # 创建索引
        self.create_index(es_service_host=self.es_service_host)

        a = 0
        actions = []

        for message in kafka_conn.consumer:

            if message is not None:
                log_value = str(message.value, encoding="utf8")

                if "_pod_name_" in log_value:   # log_version:3.4.0

                    try:
                        raw_log_dict = json.loads(log_value)
                    except Exception as e:
                        logging.error(
                            f"msg:{e},topic:{message.topic},partition:{message.partition},offset:{message.offset},received_log:{log_value}")
                        continue

                    matched_content = re.match(r"^(?P<time>.*?)\s+(?P<data>\{.+\})$", raw_log_dict["content"])

                    if not matched_content:
                        logging.error(
                            f"topic:{message.topic},partition:{message.partition},offset:{message.offset},received_log:{log_value}")
                        continue

                    log_time = matched_content.group("time").strip()
                    log_data = json.loads(matched_content.group("data"))

                    finall_value: dict = log_data["log"]
                    finall_value["logVersion"] = log_data["log_version"]
                    values_date = log_time.split(".")

                else:
                    # 旧版本日志，后期删除,  logVersion: 3.3.0
                    values = re.findall("\[(.*?)\].*?Thread](.*)", log_value)[0]

                    finall_value = json.loads(r"""{}""".format(values[1]))
                    values_date = values[0].split(".")

                finall_value["log_timestamp"] = str(
                    datetime.datetime.strptime(values_date[0], '%Y-%m-%d %H:%M:%S')).replace(" ", "T") + "," + str(
                    values_date[1])

                finall_value["requestJson"]["request"]["nlu"][0]["slots_nested"] = \
                    finall_value["requestJson"]["request"]["nlu"][0]["slots"]

                request_id = finall_value["requestJson"]["request"]["requestId"]
                timestamp = finall_value["requestJson"]["request"]["timestamp"]

                if finall_value["requestJson"]["context"]["system"]["user"]["latitude"]:
                    finall_value["location"] = finall_value["requestJson"]["context"]["system"]["user"][
                                                   "latitude"] + "," + \
                                               finall_value["requestJson"]["context"]["system"]["user"]["longitude"]

                # 增加{'abTestData': [['G0100108'], 1625481457445]
                ab_test = finall_value.get("abTestData")
                if ab_test:
                    finall_value.update(abTestData={"group": ab_test[0], "timestamp": ab_test[1]})

                actions.append({"_index": self.index_name, "_id": request_id + timestamp, "_source": finall_value})
                a += 1
                if a % 500 == 0:
                    try:
                        status = es_conn.bulk(actions)
                    except Exception as e:
                        logging.error(f"msg: {e}")
                        break

                    logging.info(f"saved log to {str(self.es_service_host)}, log_timestamp:{finall_value['log_timestamp']}, status:{status}")
                    a = 0
                    actions = []

        self.logger.warning("is_sigint_up error")
        kafka_conn.close()

    def start(self):
        """启动函数"""
        self.logger.info("*" * 24 + "\t日志同步程序启动\t" + "*" * 24)
        self._import_user_log_kafka2es()
        self.logger.warning("*" * 24 + "\t安全退出\t" + "*" * 24)
