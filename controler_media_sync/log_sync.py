
import datetime
import json
import logging
import os
import re
import signal
from collections import defaultdict
from concurrent.futures._base import wait
from concurrent.futures.thread import ThreadPoolExecutor
from copy import copy
from functools import partial

import pandas
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import settings
from common.es_helper import ElasticHelper
from common.kafka_helper import KafkaHelper
from common.oss_helper import OssHelper
from tqdm import tqdm


class SyncLogHandler(object):
    """同步日志到ELK"""

    def __init__(self, cursor_offset="latest", es_service_host=settings.ELK_ES,
                 index_name=settings.INDEX_USER_SEARCH_LOG):
        self.logger = logging
        self.group_id = settings.USER_LOG_GROUP_ID
        self.topic = settings.USER_LOG_TOPIC
        self.bootstrap_servers = settings.BOOTSTRAP_SERVERS
        self.cursor_offset = cursor_offset
        self.index_name = index_name
        self.es_service_host = es_service_host

        self.oss_url = "http://ip:port/oss/hive/list"
        self.oss_key = "cysiHIcIEJXQmq2K"
        self.oss_key_secret = "mpUY7SnOxaoms3tb"
        self.oss_data_type = "vd.ods_vd_search_log_di"

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

    def __formatting_log(self, log_dict: dict):
        """统一日志格式"""

        log_dict["requestJson"]["request"]["nlu"][0]["slots_nested"] = \
            log_dict["requestJson"]["request"]["nlu"][0]["slots"]

        request_id = log_dict["requestJson"]["request"]["requestId"]
        timestamp = log_dict["requestJson"]["request"]["timestamp"]

        log_dict["_id"] = request_id + timestamp

        if log_dict["requestJson"]["context"]["system"]["user"]["latitude"]:
            log_dict["location"] = log_dict["requestJson"]["context"]["system"]["user"][
                                       "latitude"] + "," + \
                                   log_dict["requestJson"]["context"]["system"]["user"]["longitude"]

        # 增加{'abTestData': [['G0100108'], 1625481457445]
        ab_test = log_dict.get("abTestData")
        if ab_test:
            log_dict.update(abTestData={"group": ab_test[0], "timestamp": ab_test[1]})

        return log_dict

    def import_log_from_kafka(self):
        """从kafka导入实时日志到elk"""
        kafka_conn = KafkaHelper(group_id=self.group_id, topic=self.topic,
                                 bootstrap_servers=self.bootstrap_servers, cursor_offset=self.cursor_offset)
        es_conn = ElasticHelper(server_url=self.es_service_host)

        # 创建索引
        self.create_index(es_service_host=self.es_service_host)
        a = 0
        actions = []

        for message in kafka_conn.consumer:
            if message is not None:
                log_value = str(message.value, encoding="utf8")

                if "_pod_name_" in log_value:  # log_version:3.4.0

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

                    finall_value = log_data["log"]

                    # 补充logVersion 字段
                    finall_value["logVersion"] = log_data["log_version"]
                    values_date = log_time.split(".")

                else:
                    # 旧版本日志，后期删除,  logVersion: 3.3.0
                    values = re.findall("\[(.*?)\].*?Thread](.*)", log_value)[0]

                    finall_value = json.loads(r"""{}""".format(values[1]))
                    values_date = values[0].split(".")

                # 补充log_timestamp 字段
                finall_value["log_timestamp"] = str(
                    datetime.datetime.strptime(values_date[0], '%Y-%m-%d %H:%M:%S')).replace(" ", "T") + "," + str(
                    values_date[1])

                # 统一结构化
                source = self.__formatting_log(log_dict=finall_value)
                data_id = source.pop("_id")

                actions.append({"_index": self.index_name, "_id": data_id, "_source": source})
                a += 1
                if a % 500 == 0:
                    try:
                        status = es_conn.bulk(actions)
                    except Exception as e:
                        logging.error(f"msg: {e}")
                        break

                    logging.info(
                        f"saved log to {str(self.es_service_host)}, log_timestamp:{finall_value['log_timestamp']}, status:{status}")
                    a = 0
                    actions = []

        self.logger.warning("is_sigint_up error")
        kafka_conn.close()

    def import_log_from_oss(self, start_date, end_date=None):
        """从os_s导入指定日期日志到elk"""
        if end_date is None:
            end_date = start_date

        oss_helper = OssHelper(url=self.oss_url, key=self.oss_key, key_secret=self.oss_key_secret,
                               data_type=self.oss_data_type)
        executor = ThreadPoolExecutor(max_workers=8)
        all_tasks = []
        actions = []
        es_conn = ElasticHelper(server_url=self.es_service_host)

        def on_done(f, latest_time):
            e = f.exception()
            if e:
                print(f"msg: {e}")
                return
            tqdm.write(f"saved log to {str(self.es_service_host)}, log_timestamp:{latest_time}, status:{f.result()}")

        for date in pandas.date_range(start=str(start_date), end=str(end_date)):
            date = date.strftime("%Y%m%d")
            download_fields = oss_helper.start_download(date)
            if not download_fields:
                return
            self.logger.info(
                f"downloaded {len(download_fields)} files, filepath {os.path.join(oss_helper.path_dir, date)}")
            log_iterator = oss_helper.read_parquet_date(date)

            for ori_log in log_iterator:
                # 记录数据来源和脚本版本

                # ori_log = {'search_date': '2021-10-17',
                #            'search_time': '2021-10-17 00:03:54',
                #            'queryid': 'health-check-1634400234246',
                #            'log_json': '{"id":"health-check-1634400234246","log":{"abTestData":[],"costTime":"53","esSearchTime":"43","host":"aisearch-694fd754df-9p2kh","level":"INFO","loggerName":"com.tcl.aisearch.common.thread.WriteLogThread","message":"VUI5","posters":[],"previouslyMovie":[["15","mzc00200uf2vwkp",""],["15","mzc00200y72jo65",""],["15","mzc00200mt43s5r",""],["15","babks6vmrn25n9c",""],["15","mzc00200otibal3",""],["15","tk3l27paq7sqr0z",""],["15","mzc00200bmxe3ek",""],["15","mzc00200kqecmvk",""],["15","mzc002001q141h4",""],["15","mzc00200uvgh0p1",""],["15","mzc00200yuiy84o",""],["15","s5gyg482zt8zf5m",""],["15","mzc002001clmhji",""],["15","mzc00200h5bpdgr",""],["15","mzc00200h97lhuv",""],["15","mzc00200qa6e0k8",""],["15","5cn5yhezzjeda4b",""],["15","1bnyfttijufe0cq",""],["15","pv1noxn2wtgzash",""],["15","mzc00200cpg1ig3",""],["15","piegqd6mz97tr8b",""],["15","8kw2uo89elwb7as",""],["15","mzc0020094d291y",""],["15","mzc002009z91df5",""],["15","nbusftujqwczi7y",""]],"requestJson":{"context":{"system":{"callbackurl":"","device":{"clientType":"health-check-dmdl","deviceId":"health-check-did","launcherAppVer":"","mode":"","sdkVersion":400008},"simple":false,"user":{"accessToken":"","latitude":"","longitude":"","queryid":"health-check-1634400234246","userId":"health-check-uid"},"video":{"dnum":"","mitu":{"vip":false}}}},"request":{"dialogState":"string","key":"1F46CB9EA39544C9BB1BD2B9E6221248","nlu":[{"domain":"movie","intent":"Movie.Search","skill":"TCL_MOVIE_SEARCH","slots":[{"name":"MovieType","value":"电影"}]}],"queryText":"电影","requestId":"health-check-1634400234246","timestamp":"1634400234","type":"IntentRequest"},"session":{"new":false,"sessionId":""}},"resources":[["15","mzc00200uf2vwkp",""],["15","mzc00200y72jo65",""],["15","mzc00200mt43s5r",""],["15","babks6vmrn25n9c",""],["15","mzc00200otibal3",""],["15","tk3l27paq7sqr0z",""],["15","mzc00200bmxe3ek",""],["15","mzc00200kqecmvk",""],["15","mzc002001q141h4",""],["15","mzc00200uvgh0p1",""],["15","mzc00200yuiy84o",""],["15","s5gyg482zt8zf5m",""],["15","mzc002001clmhji",""],["15","mzc00200h5bpdgr",""],["15","mzc00200h97lhuv",""],["15","mzc00200qa6e0k8",""],["15","5cn5yhezzjeda4b",""],["15","1bnyfttijufe0cq",""],["15","pv1noxn2wtgzash",""],["15","mzc00200cpg1ig3",""],["15","piegqd6mz97tr8b",""],["15","8kw2uo89elwb7as",""],["15","mzc0020094d291y",""],["15","mzc002009z91df5",""],["15","nbusftujqwczi7y",""],["15","ohk0itiuqoi5mf6",""],["15","mzc00200z8hg7m6",""],["15","mzc00200rblzjix",""],["15","msbt3iret5tgwu8",""],["15","1v1d6tnz4ugk7kg",""],["15","l9vxrwfdjcw1q4g",""],["15","mzc002001edspb4",""],["15","ge1vg9evjfeb6l0",""],["15","mzc0020021sbml5",""],["15","j22edv0k6xtn65p",""],["15","fi3l3hu8mfcxxyu",""],["15","3fvg46217gw800n",""],["15","mzc00200ju34z10",""],["15","wi8e2p5kirdaf3j",""],["15","5lmuabxmb5jf1fl",""],["15","380idj4s3fxn1mz",""],["15","qxlvauamtoi399d",""],["15","jbz487z0v6b23gm",""],["15","mzc002009qfqpu0",""],["15","om43f374meb25iu",""],["15","mzc00200mw60fd9",""],["15","mzc002002zer44h",""],["15","e0bk8kf7wllv7r8",""],["15","pbjxhj5m3a4sd3d",""],["15","7fjv56lqblm6jhd",""],["15","xyaud3hk6mmi6ss",""],["15","3v5ksc6jiwskqw4",""],["15","3rscgjtv6jqzm9y",""],["15","9f8tpq2gly9p5ts",""],["15","mzc00200q2kk474",""],["15","mzc00200fw94jce",""],["15","mzc00200yu62ksg",""],["15","mzc00200mykcc34",""],["15","mzc002005bvno3g",""],["15","mzc00200vkdojwu",""],["15","10evt1f9wqxa8bq",""],["15","tbs8gg4bepxyp2y",""],["15","llcj2tlwn1jn8y7",""],["15","4xf4ni3vwii9kl1",""],["15","mzc002006ncxgx1",""],["15","q8xwiu8yyxahzz8",""],["15","mzc002003htxuzy",""],["15","v2098lbuihuqs11",""],["15","v3sfrz8ws9ew7fw",""],["15","mzc00200ckwg9tt",""],["15","3o56brnhunm5bwx",""],["15","zr5a67l333ehzu9",""],["15","3xk72twalaqaku0",""],["15","ujnamwpqg1xg8qm",""],["15","gz9q7kd8wviwkxv",""],["15","mzc00200qfarw6d",""],["15","gothgwl98bmq6es",""],["15","mzc00200mv2vcc5",""],["15","t2t7544bcr1pw8r",""],["15","mzc00200ylulwyn",""],["15","b40ff7kummjfp7e",""],["15","mzc002003k94702",""],["15","r9umzdo0yjelfg1",""],["15","f7pqur8uhmzltps",""],["15","mzc00200kgu26te",""],["15","mzc00200vx37bvf",""],["15","4ins96adw2a5rfv",""],["15","20lmr7rqqw8hxo3",""],["15","mzc00200dszoxim",""],["15","phk4s9bkfs8xdn4",""],["15","os7pc1allqs7luc",""],["15","rtxppsa2dqxd6ok",""],["15","mzc0020067a704x",""],["15","uuv2z9lhi1akhjq",""],["15","xgyd6mjl7vo6kpb",""],["15","42x9x33lueay91y",""],["15","ii4644hvs8zijia",""],["15","zxsr2hae16abq7v",""],["15","g6x5mmrcdgl0n3x",""],["15","hu9th0blgco6c89",""],["15","we35g3aduiwkudp",""],["15","7casb7nes159mrl",""],["15","mzc00200gyw6u04",""],["15","mzc00200mhyxg8f",""],["15","mzc00200oxq32bf",""],["15","deyzhnzo65ik79t",""],["15","jpjl78i91mx05bu",""],["15","mzc002007g7aqqt",""],["15","mzc00200tal4xbg",""],["15","ggaq10nuimpw3u8",""],["15","ccd36tevohjt9ze",""],["15","mzc00200hlpzh3r",""],["15","mzc002009jjvlgx",""],["15","8f57gnkxwj5trnq",""],["15","bojb6fxtqh2ekw0",""],["15","kkpdn0lz6drhxz8",""],["15","7hg5nr3x10ingpj",""],["15","d29dakv9k1xy9p9",""],["15","ly2d18qrdchs2mm",""],["15","3au50sy4i90heio",""],["15","muu0r5j9a0m4wh4",""],["15","mzc00200om4y6y0",""],["15","luxx8xe8hybi2vp",""],["15","ra1dhxmhz872ry8",""],["15","wtr1lrmk9w72j6x",""],["15","z6j3ixjjcokafyc",""],["15","f3qroleq1lyuavy",""],["15","r6ri9qkcu66dna8",""],["15","71bctb897dwx46m",""],["15","mzc00200t25wdd4",""],["15","lcewsuqxacvnvh5",""],["15","53el1qwq1gnm5fn",""],["15","o66no96ex54bksd",""],["15","mzc00200892n01l",""],["15","36t1wi1qri4h2ej",""],["15","tpa0c309erq2p7t",""],["15","mzc00200o5otmir",""],["15","6xzlbrzswkuo3wq",""],["15","5e1o97rl37tjzbt",""],["15","5y95zy4idzqf6hc",""],["15","mzc00200irwp8b1",""],["15","q36vxbd95odijk1",""],["15","mzc00200pvnvlit",""],["15","8zqwf5rgbiq4kam",""],["15","0w0zgqo2jmpenhf",""],["15","t9fpyv7bdmg2oyp",""],["15","16cms7sv5mymn7u",""],["15","803p673mlosoeog",""],["15","r2eatcdcv2deik5",""],["15","jm7ingmp113qaqu",""],["15","mzc00200ru5uzko",""],["15","ccpvcymj1aw4iuh",""],["15","laraofctuh3mh4q",""],["15","uxcy3k044fczp2j",""],["15","mzc002000149ian",""],["15","h188mdjtrh3up22",""],["15","mzc00200rj173g9",""],["15","mu01sbah4rauf44",""],["15","unhg39knn3fjkb7",""],["15","mzc00200kqaoxob",""],["15","wmey1i8mowpiiqu",""],["15","ba4l99vukuzjanj",""],["15","4myi5m71d14pdmr",""],["15","muhmpuy3wzgqcur",""],["15","hk765ykwj4bjpcl",""],["15","5oy778gc6ua4h70",""],["15","w3ul47d11rav8ks",""],["15","bv8m91i5jj1w1ww",""],["15","xq40wik5tkvqcrv",""],["15","nvy9lp6e061ecu2",""],["15","zww0q1hojoy462g",""],["15","hzssbvhzc7xyr95",""],["15","hrr3qneqbo22q06",""],["15","bkzp1k99vtmpiie",""],["15","fse52rd4klx7qn2",""],["15","rj5vksslchkvh63",""],["15","m5zzglrbt5zdv6d",""],["15","h05z5bsjxw544er",""],["15","1dc6xsgwwa0t98c",""],["15","mzc002003gzadbo",""],["15","owyequak2ck3bra",""],["15","p23rohsggrrcgdz",""],["15","uecexs5vdqcsfkf",""],["15","0k89cbc5x5j5q4o",""],["15","fcw34uctzd173ii",""],["15","mzc00200f6xy9ua",""],["15","hdavca20ujfapqy",""],["15","mzc00200pcxqtjm",""],["15","bqejbbv8mgtusrs",""],["15","2m5reall3p0yj8r",""],["15","c3jiqn1oso8knul",""],["15","wagzbx91asjomnu",""],["15","mzc00200dabqlu0",""],["15","gpcfwzyrfzyalj7",""],["15","mzc00200t5tzp1v",""],["15","kep6vp3yaheymko",""],["15","6pfpvdk6z9v55wc",""],["15","96sxjj429lictza",""],["15","6wx9pvukqzx9mkx",""],["15","mzc00200ce4e8gi",""]],"resultCode":"0","searchPath":[4,5],"serverVersion":"3.4.1","shortVideo":[["15","mzc00200haf292w",""],["15","c949qjcugx9a7gh",""],["15","zm91ry6rctntum8",""],["15","mzc00200o4emst0",""],["15","mzc00200rkrivfm",""],["15","mzc00200iwussiq",""],["15","q4enmfhbv50hqnc",""],["15","mzc002001wz0zi0",""],["15","mzc00200yr0t8d1",""],["15","mzc00200xkxid9y",""],["15","bzzfw21ipn6v1f0",""],["15","3hcnhz6ys9njhun",""],["15","1nawx3148rx2apm",""],["15","s2i11p57w7ae84g",""],["15","mzc00200qq2u4yw",""],["15","jw5c4ve48xlvao6",""],["15","9d71oiz9jq262jk",""],["15","liyb662gbkjxu9k",""],["15","xml1o6owv334gix",""],["15","6nszx1gt6yvu3b6",""],["15","pd8bmnlmrogoyhr",""],["15","2rgkj31016tqmhf",""],["15","mzc00200pu3i5gk",""],["15","06f338d6r1wa3my",""],["15","84cu4uxrkzi0e60",""],["15","mzc00200k5m3li2",""],["15","mzc00200md2pccz",""],["15","188iqx9qgt09bol",""],["15","xejsxrzqxpqw7cd",""],["15","g1gf358prepzhvb",""],["15","r76vhwod2ok8nvo",""],["15","k8nhr99yzsq472z",""],["15","mzc00200ece8uao",""],["15","mzc00200yumuikq",""],["15","bq1opywtlv8tvzd",""],["15","mzc00200hahowa3",""],["15","mzc002001gn4xue",""],["15","g8wkyyyws1zalpj",""],["15","mzc00200ahsjshj",""],["15","rxxukzqx79ae30o",""],["15","y8luepylwoq6zfj",""],["15","jshhaijes6d0b50",""],["15","dxejskxh9kkcry3",""],["15","v1bclez1csji5za",""],["15","mzc00200rv9z23s",""],["15","wq8gq50ujc31t0l",""],["15","ssdv08vz29x2v9l",""],["15","1lq0xx564i3g9rs",""],["15","mzc00200tvh98m0",""],["15","w8na1mzcmyqqqsm",""],["15","mzc002009mbu72q",""],["15","mzc002008ic615v",""],["15","m5vvbogykkprnbq",""],["15","mzc002008ig6qug",""],["15","7rt31c0md6vq22k",""],["15","2jyug8pgwoqgt5d",""],["15","mzc00200jiheal7",""],["15","kihit8es94tsv2d",""],["15","mzc0020075bvt9d",""],["15","mzc00200onuxyf1",""],["15","8t8h4kp2cap2iw0",""],["15","1b6sb66yrjgrfj8",""],["15","mzc002005193ltf",""],["15","mzc002007a3011y",""],["15","mzc002001m5wxlo",""],["15","mclhefnsprerk8n",""],["15","shr6fmb3cra8r5o",""],["15","2tk2e2v12j4jcm5",""],["15","se6sn2py113phnt",""],["15","mzc00200ewgi3o5",""],["15","vzz0pu2m1s2kghs",""],["15","6o5nullcebo1jys",""],["15","2j5kilwvtepehti",""],["15","mzc002000miwtu7",""],["15","mzc00200hcj45oq",""],["15","mzc00200jepuvw0",""],["15","mzc0020053jrtu1",""],["15","jymyehve4i7nmqr",""],["15","mzc002006ca87j5",""],["15","ifggdul2vkci5ni",""],["15","mzc002004bmxanl",""],["15","4ihzhcq97xow00p",""],["15","mzc00200x2too2j",""],["15","cmv67rzhrf5s5r3",""],["15","rxgqfwv6k9a09ab",""],["15","0py2pytt6n8l0ci",""],["15","mzc00200p0hmg0l",""],["15","zatx0zuk94d9aru",""],["15","mzc002008cs4e6q",""],["15","zr0p8nl0i6w9x7s",""],["15","1bm41dja9yziv75",""],["15","rlfsj5swk59fhpy",""],["15","7imx81cvj1fgzho",""],["15","mzc00200k5od8qx",""],["15","qsmjp87yyoqm7fy",""],["15","wl13mh8iy1lj321",""],["15","81ncrlb0n9uru2l",""],["15","c99ep8f0vhvy6kb",""],["15","mzc0020028aofft",""],["15","hopsypnhxk1kzr1",""],["15","d8t20577qy9vy13",""],["15","6ku0ttyetym0yke",""],["15","mzc00200c20yyiw",""],["15","xgybdwzaljwblg0",""],["15","mzc00200w24ex37",""],["15","cqa1l6fj5d49fvw",""],["15","mzc00200fvtzd8j",""],["15","mzc00200r0t8bqq",""],["15","5xumgsg8k780yk0",""],["15","mzc00200mziv37g",""],["15","mzc00200dstql3i",""],["15","hxgrzg88w7do4b9",""],["15","db2e4g0eyqrenbn",""],["15","r67q7e9pr68p4jr",""],["15","vyu8wffdigwg6bq",""],["15","novy1d6imtjd0xv",""],["15","mzc00200x1flkxq",""],["15","mzc002000r252z5",""],["15","2lznt4nq37pry3m",""],["15","0y7vz6mjs90210p",""],["15","zlc5kj7b9w059t5",""],["15","7cttdsfe3kp34ah",""],["15","vc4q8urecxq5oxh",""],["15","026g4k0riiec2ox",""],["15","p7z3o1mrr803mvo",""],["15","1v3nxtz4n7h9iit",""],["15","mzc00200x6zit2c",""],["15","mzc00200q30xvks",""],["15","d0womh06pzk3h9k",""],["15","pnri9ni4cbvqama",""],["15","bzndmy0i9dx04wr",""],["15","7yfixglpirh0v1e",""],["15","gd7fvcxfj1egx5b",""],["15","mzc00200lv9lw68",""],["15","mzc00200wluvp2q",""],["15","x3ltpzdyzahcz8w",""],["15","mzc00200lolarfg",""],["15","w9roxax9ciozcs9",""],["15","o6iafpoet9y7ccn",""],["15","zwwzcs2u2k17qq0",""],["15","mzc00200dinyglr",""],["15","fsgnn0wrkn17ret",""],["15","r9hn2ijmzukqaa9",""],["15","mzc002004vwfi61",""],["15","7qh7v7k0temxg9r",""],["15","x3cntyd9hz21ug8",""],["15","te6fzfvdnzvohrt",""],["15","mzc00200xzlhpxl",""],["15","agq0ydhoxzitatw",""],["15","rwyy9sfjp6nkwv1",""],["15","c2h3m70s4q41ank",""],["15","mzc002008todwel",""],["15","mu2wmmquzlgk6p8",""],["15","mzc0020060ac7j9",""],["15","shej7grsnosgq52",""],["15","v994issfff6wut0",""],["15","jaa024gf6wdnjrl",""],["15","zcoz8opuklm5ue3",""],["15","mzc00200rqrgvfk",""],["15","ljhvmqvqac0tlra",""],["15","jwrzn5nsjx8g0vv",""],["15","76n6vdky9nfocyg",""],["15","mzc00200bvjpdlj",""],["15","cekevivydsglc3z",""],["15","mzc0020049otqyv",""],["15","n6m61b7hzs3lf1w",""],["15","mzc002005b95znv",""],["15","nqmxae531yjmbua",""],["15","mzc002009epob7a",""],["15","htua0owyrcinp5c",""],["15","3u551qgyjhil9l7",""],["15","3nd6v5g14aoo20t",""],["15","73b7a2o691nwf3b",""],["15","51vqsezg9y49726",""],["15","bnb06eqld28sgc4",""],["15","tqrcx71evgmlcvx",""],["15","mvb5pimlitsb2rf",""],["15","ipyt50vg5ee4q7x",""],["15","rbrehmwm1sv2ufz",""],["15","sgt1s17mieejpl3",""],["15","i0ispxj6p0sih58",""],["15","epwv9r9fy1mcqqk",""],["15","mzc002009t9rdfv",""],["15","8o38jlbelgp6xt8",""],["15","mzc00200emmamia",""],["15","g6gb19qe6uvdbu8",""],["15","ghl33amcodwehkt",""],["15","mzc00200f85ound",""],["15","yck2essn6qeml70",""],["15","mzc00200so40ce8",""],["15","iudlk99qmz0o363",""],["15","i8h5cwa8w374wda",""],["15","2482j70u4wnvazn",""],["15","fu27eggvos116xd",""],["15","49bje65es1laii7",""],["15","38e10yetrv3g0tj",""],["15","fus18p37381jh7t",""],["15","8olndfsmrmr46hm",""],["15","6qs1v8gkdefarxc",""],["15","j2ea1484cunnq95",""]],"timestamp":"1634400234306","tts":"电影"},"log_version":"3.4.0"}',
                #            'log_version': '3.4.0',
                #            'imp_date': '20211017'}
                try:
                    log_dict = json.loads(ori_log["log_json"])["log"]
                except Exception as e:
                    print(f"msg: {e}, content: {ori_log}")

                # 补充logVersion 字段
                log_dict["logVersion"] = ori_log["log_version"]
                # 补充log_timestamp 字段
                log_dict["log_timestamp"] = ori_log["search_time"].replace(" ", "T")

                source = self.__formatting_log(log_dict=log_dict)
                data_id = source.pop("_id")
                actions.append({"_index": self.index_name, "_id": data_id, "_source": source})
                latest_timestamp = log_dict["log_timestamp"]

                if len(actions) >= 500:
                    future = executor.submit(es_conn.bulk, bulk_actions=copy(actions))
                    future.add_done_callback(partial(on_done, latest_time=latest_timestamp))
                    actions = []

                    all_tasks.append(future)
                    if len(all_tasks) >= 100:
                        # 避免创建大量任务导致内存泄漏
                        wait(all_tasks)
                        all_tasks = []

                if self.is_sigint_up:
                    break
        executor.shutdown()


if __name__ == '__main__':
    handler = SyncLogHandler()
    handler.import_log_from_oss(start_date="20210906", end_date="20210907")
