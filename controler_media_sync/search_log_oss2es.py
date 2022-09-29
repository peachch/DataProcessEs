
import logging
import os
import re
import shlex
import signal
import subprocess
import time
from collections import defaultdict
from concurrent.futures.thread import ThreadPoolExecutor
from threading import Lock

import pandas as pd
import requests
import json
import datetime
from retry import retry

import settings
from common.es_helper import ElasticHelper

lock = Lock()


class SyncOssUserLog(object):
    def __init__(self, sync_date, es_service_host=settings.ELK_ES, index_name=settings.INDEX_USER_SEARCH_LOG):
        """dnum数据同步到线上环境"""
        self.sync_date = sync_date
        self.index_name = index_name
        self.oss_api = ""
        self.logger = logging
        self.es_service_host = es_service_host

        # if target_es_env is None:
        #     # 默认发布到生产和预生产
        #     self.target_es_env = [settings.PRODUCTION_ENV, settings.PREPRODUCTION_ENV]

        # 记录已经同步完的日期
        self.sync_completed_date = defaultdict(lambda: False)
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
        logging.warning('Catched interrupt signal!')
        self.sigint_up_count += 1
        if self.sigint_up_count >= 3:
            # 强制退出
            try:
                os._exit(0)
            except:
                print('Program is dead.')
            finally:
                print('clean-up')

    @retry(tries=3)
    def get_download_urls(self, date):
        """获取下载链接"""
        while True:
          payload = json.dumps({"date": date, "dataType": "vd.ods_vd_search_log_di"})
          headers = {
              'Content-Type': "application/json",
              'key': "",
              'keySecret': "",
          }
          resp_dict = requests.request("POST", url=self.oss_api, data=payload, headers=headers).json()
          if resp_dict["message"] == "Success":
            break
          else:
            continue

        download_urls = resp_dict.get("result", [])
        if download_urls:
          self.logger.info(f"Get download urls: {download_urls}")

        return download_urls

    def wget_fetch(self, download_url, file_path):
        """调用wget下载数据"""
        file_name = re.search(r"/parquet/(?P<filename>.+)\?", download_url).group("filename")
        save_path = os.path.join(file_path, file_name)

        cmd = f'wget --tries=3 --timeout=60 --output-document="{save_path}" "{download_url}"'
        cmd_list = shlex.split(cmd)
        # from python lib manual
        # Run the command described by args. Wait for command to complete, then return a CompletedProcess instance.
        cp = subprocess.run(cmd_list)
        if cp.returncode != 0:
            self.logger.error(f'Download fail; url:{download_url}')
            return None

        # 实际上只会下载一个文件
        self.logger.info(f"Download success; file: {save_path}")
        return save_path

    @retry(tries=3)
    def read_dnum_data(self, filepath):
        """读取dnum"""
        dnum_df = pd.read_parquet(path=filepath)

        return list(zip(dnum_df["search_time"], dnum_df["log_json"]))

    def create_index(self, target_es: ElasticHelper):
        """创建索引"""
        # TODO 需要接入米兔数据，索引结构需要更新

        if not self.es_created_index[target_es.__str__()]:
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
                    "iot":{
                      "properties":{
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
        "location":{"type":"geo_point"},
      }
    }}
            target_es.create_index(index_name=self.index_name, configuration=configuration)
            self.es_created_index[target_es.__str__()] = True

    def import_dnums_to_es(self, dnums, target_es: ElasticHelper):
        """导入数据到索引"""
        sources = []
        for dnum in dnums:
            update_time = dnum[0].replace(" ", "T")
            source = json.loads(dnum[1])
            # update_time = str(datetime.datetime.now()).replace(" ", "T").replace(".", ",")
            source["log_timestamp"] = update_time
            source["requestJson"]["request"]["nlu"][0]["slots_nested"] = \
            source["requestJson"]["request"]["nlu"][0]["slots"]

            request_id = source["requestJson"]["request"]["requestId"]
            timestamp = source["requestJson"]["request"]["timestamp"]


            if source["requestJson"]["context"]["system"]["user"]["latitude"]:
                source["location"] = source["requestJson"]["context"]["system"]["user"][
                                               "latitude"] + "," + \
                                           source["requestJson"]["context"]["system"]["user"]["longitude"]

            # 日志结构构建

            sources.append({"_index": self.index_name, "_id": request_id + timestamp, "_source": source})
            # sources.append(source)
        target_es.bulk(sources)
        # target_es.update_index_bulk(index_name=self.index_name, sources=sources)
        return dnums



    def update_dnum(self):
        """更新总体逻辑实现"""
        current_date = self.sync_date
        is_completed = False

        # 获取下载链接
        download_urls = self.get_download_urls(current_date)
        if not download_urls:
            return is_completed

        # 创建目录
        dnum_path_today = os.path.join(os.path.join(settings.BASE_DIR, "oss_user_log"), current_date)
        os.makedirs(name=dnum_path_today, exist_ok=True)

        # TODO 改为多进程,windows下多进程数据库连接对象无法序列化
        download_completed_file = []
        for download_url in download_urls:
            result_path = self.wget_fetch(download_url=download_url, file_path=dnum_path_today)
            if result_path:
                download_completed_file.append(result_path)



        imported_dnums_record = []
        dnums_count = 0

        es_conn = ElasticHelper(server_url=self.es_service_host)

        def on_done(future):
            """显示导入进度"""
            with lock:
                imported_dnums_record.extend(future.result())
                imported_count = len(imported_dnums_record)
                process_rate = int(100 * (imported_count / dnums_count))
                # 打印进度
                print(
                    f"Importing dnum: {process_rate}%|{'█' * process_rate + ' ' * (100 - process_rate)}| {imported_count}/{dnums_count}",
                    end="\r" if process_rate < 100 else "\n")

        for dnum_file_path in download_completed_file:
            dnums = self.read_dnum_data(dnum_file_path)

            dnums_count += len(dnums)

            # 多线程导入
            max_workers = 12
            cut_step = 4000
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                for idx in range(0, len(dnums) + 1, cut_step):
                    f = executor.submit(self.import_dnums_to_es, dnums[idx:idx + cut_step], es_conn)
                    f.add_done_callback(on_done)


        self.logger.info(f"Complete import dnum, total:{len(imported_dnums_record)}/{dnums_count}")


        return is_completed


    def start(self):
        """启动函数"""
        self.logger.info("*" * 24 + "\toss同步程序启动\t" + "*" * 24)
        self.update_dnum()
        self.logger.warning("*" * 24 + "\t安全退出\t" + "*" * 24)
