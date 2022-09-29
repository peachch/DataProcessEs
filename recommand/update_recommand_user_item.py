import shutil

import elasticsearch
from elasticsearch import Elasticsearch, helpers
import pypinyin
import click

# 获取dnum和其对应的dnum_index
import time
import json
import oss2
import os
import pandas as pd
from apscheduler.util import undefined
import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
import tarfile
import time
from concurrent.futures.thread import ThreadPoolExecutor
from threading import Lock, Thread
from retry import retry
from tqdm import tqdm

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

@click.group(invoke_without_command=False)
def cli():
    pass


elastic_conn1 = Elasticsearch(hosts=[{'host': '', 'port': 0}], timeout=1000, http_auth=None)
elastic_conn2 = Elasticsearch(hosts=[{'host': '', 'port': 0}], timeout=1000, http_auth=None)


def get_es_conn(env):
    """获取不同环境es连接"""
    if env == "develop_env":
        elastic_conn = Elasticsearch(hosts=[{'host': '', 'port': 0}], timeout=1000, http_auth=None)
        print("成功连接")
    elif env == "preproduction_env":
        elastic_conn = Elasticsearch(hosts=[{'host': '', 'port': 0}], timeout=1000, http_auth=None)
        print("成功连接")
    elif env == "production_env":
        elastic_conn = Elasticsearch(hosts=[{'host': '', 'port': 0}], timeout=1000, http_auth=None)
        print("成功连接")
    else:
        raise Exception(f"{env} is unavailable")
    return elastic_conn


class FeatureHandler(object):
    _user_id_dict=None
    es_list = []
    day_of_week = "mon"
    hour = "12"
    minute = "40"

    def __init__(self):
        assert self._user_id_dict is not None

        self._auth = oss2.Auth('', '')
        self._bucket = oss2.Bucket(self._auth, '', '')
        self._current_time = (datetime.datetime.now().date() - datetime.timedelta(days=1)).strftime('%Y%m%d')
        # self._current_time = (datetime.datetime.now().date()).strftime('%Y%m%d')
        self._oss_f_path = "/"
        self._done_file = None
        self._feature_user_file_name = None
        self._feature_user_file_name_untar = None
        self._oss_done_file_path = self._oss_f_path + self._current_time + '.done'
        self._oss_download_url = None
        self._tag_idx_dict = None
        self._user_id_dict = None
        self._target_index = f"recommend_user_feature_{self._current_time}test_user_feature"


    def download(self):
        # file_name = "feature_user.tar.gz"
        # file_path = self._f_path + self._current_time + '.done'
        exist = self._bucket.object_exists(self._oss_done_file_path)
        if exist:
            self._bucket.get_object_to_file(self._oss_done_file_path, self._done_file)
            with open(self._done_file, "r") as file_done:
                content = [file.strip('\n') for file in file_done.readlines()]
                if self._feature_user_file_name in content:
                    # url = self._f_path + self._current_time + "/" + file_name
                    print(self._oss_download_url)
                    try:
                        # feature_user_dir = os.path.join()
                        # if not os.path.exists(self._feature_user_tar_path):
                        #     self.path_dir_mk = os.makedirs(self._feature_user_tar_path)
                        download_feature_tar_path = os.path.join(self._feature_user_tar_path, self._feature_user_file_name)
                        self._bucket.get_object_to_file(self._oss_download_url, download_feature_tar_path)
                        # for old_file in os.listdir(f"./{self._feature_user_tar_path}/{self._feature_user_file_name}"):
                        #     os.remove(old_file)
                        # feature_user_dir =
                        untar_dir_path = os.path.join(self._feature_user_tar_path, self._feature_user_file_name)
                        tar = tarfile.open(untar_dir_path, 'r')
                        tar.extractall(path=os.path.join(self._feature_user_tar_path, self._feature_user_file_name_untar))
                        tar.close()

                    except Exception as e: print(repr(e))
                else:
                    print(self._current_time + "的数据没有更新，等待明天更新")


    def get_tag_idx(self, tag_info_path):
        # 生成tag和idx对应的字典
        with open(tag_info_path, "r", encoding="utf-8") as f:
            tag_idx_dict = json.load(f)
        print("tag总数",len(tag_idx_dict))
        return tag_idx_dict


    def get_dnum_idx(self, user_info_path):
        user_id_dict = {}
        final_dnum_list = []
        with open(user_info_path, "r") as f:
            dnum_list = json.load(f)
            print(dnum_list[0])
            for dnum_dict in dnum_list:
                user_id = dnum_dict["dnum"]
                user_id_index = dnum_dict["idx"]

                user_id_dict[user_id] = user_id_index
                final_dnum_list.append(user_id)
        print("user_info中的dnum数据总量", len(final_dnum_list))
        return user_id_dict


    def get_user_profile(self,user_profile_path):
        dir_list = os.listdir(user_profile_path)
        user_info_list = []
        for parquet_file in dir_list:
            if parquet_file != "_SUCCESS":
                frame = pd.read_parquet(f"{user_profile_path}/{parquet_file}",engine='pyarrow')
                # print(frame.head(3))
                df_as_json = frame.to_json(orient='records', lines=True)
                for json_document in df_as_json.split('\n'):
                    _source = json.loads(json_document)
                    user_info_list.append(_source)
        print("得到的画像数量",len(user_info_list))
        return user_info_list


    def get_user_info(self,user_profile_path,user_id_dict,tag_idx_dict):
        for need_to_update_env in self.es_list:
            def get_bulks_update(user_info, user_id_dict, tag_idx_dict):
                bulks=[]
                dnum = user_info["dnum"]
                user_channel = user_info["channel"]
                user_recall_tags = user_info["tags"]
                user_channel_pinyin = pypinyin.slug(user_channel, separator="")
                # user_id_index = dnum_idx[user_channel]
                user_rank_tags_list_final = []

                rank_tag_string = ""
                # user_recall_tags_list = literal_eval(user_recall_tags[0])
                for i_json in user_recall_tags:
                    # i_json = json.loads(i)
                    user_rank_tags_list_final.append(i_json["tag"])
                # print(user_rank_tags_list_final)
                try:
                    user_idx = user_id_dict[dnum]
                    if len(user_rank_tags_list_final) >= 20:

                        for tag in user_rank_tags_list_final[1:21]:
                            # kkk += 1
                            if rank_tag_string == "":
                                try:
                                    rank_tag_string = str(tag_idx_dict[tag])
                                except:
                                    rank_tag_string = str(0)
                            else:
                                try:
                                    # tag_idx_dict[tag]:
                                    rank_tag_string = rank_tag_string + " " + str(tag_idx_dict[tag])
                                except:
                                    rank_tag_string = rank_tag_string + " " + str(0)
                                # rank_tag_string = rank_tag_string + " " + str(tag_idx_dict[tag])

                    else:
                        while len(user_rank_tags_list_final) < 20:
                            user_rank_tags_list_final.append("PAD")
                        for tag in user_rank_tags_list_final:
                            if rank_tag_string == "":
                                try:
                                    rank_tag_string = str(tag_idx_dict[tag])
                                except:
                                    rank_tag_string = str(0)
                            else:
                                try:
                                    # tag_idx_dict[tag]:
                                    rank_tag_string = rank_tag_string + " " + str(tag_idx_dict[tag])
                                except:
                                    rank_tag_string = rank_tag_string + " " + str(0)
                                # rank_tag_string = rank_tag_string + " " + str(tag_idx_dict[tag])

                    bulks.append(
                        {"_id": user_channel_pinyin + str(dnum), "_index": self._target_index, "user_id": dnum,
                         "channel": user_channel,
                         "user_id_index": user_idx, "channel_index": channel_idx[f"{user_channel}"], "status": 1,
                         "recall_tags": user_recall_tags, "rank_tag_indices": rank_tag_string, "timestamp": timestamp})

                    # elasticsearch.helpers.bulk(need_to_update_env, bulks)
                except:
                    pass
                # print(bulks)
                return bulks

            channel_idx = {'纪录片': 1, '电影': 2, '综艺': 3, '电视剧': 4, '少儿': 5, '动漫': 6}
            user_info_list = self.get_user_profile(user_profile_path)
            timestamp = int(round(time.time() * 1000))

            def ondone(future):
                elasticsearch.helpers.bulk(need_to_update_env, future.result())
                bar_request_api.update()

            bar_request_api = tqdm(desc="updating user", total=len(user_info_list))
            bar_request_api.set_lock(lock=Lock())
            with ThreadPoolExecutor(max_workers=8) as executor:
                for user_info in user_info_list:
                    f = executor.submit(get_bulks_update, user_info, user_id_dict, tag_idx_dict)
                    f.add_done_callback(ondone)
            bar_request_api.close()
            # return bulks

    def start_feature(self):
        """处理特征数据"""
        # 1.
        # 2.
        # 3.


        raise NotImplementedError

    def start_scheduler(self, run_now=True):
        next_run_time = undefined
        if run_now:
            next_run_time = datetime.datetime.now()
        scheduler = BlockingScheduler()
        scheduler.add_job(self.start_feature, "cron", day_of_week=self.day_of_week, hour=self.hour, minute=50,
                          next_run_time=next_run_time)
        scheduler.start()


class UserFeature(FeatureHandler):

    def __init__(self):
        self._feature_user_tar_path = os.path.join(BASE_DIR, "feature_user",self._current_time )
        self._done_file = os.path.join(self._feature_user_tar_path, f"{self._current_time}.done")
        self._feature_user_file_name = "feature_user.tar.gz"
        self._feature_user_file_name_untar = "feature_user"
        self._oss_download_url = os.path.join(self._oss_f_path , self._current_time , self._feature_user_file_name)
        self._user_profile_path = os.path.join(self._feature_user_tar_path , self._feature_user_file_name_untar, "user_profile")
        elastic_conn = Elasticsearch(hosts=[{'host': '', 'port': 0}], timeout=1000, http_auth=None)
        elastic_conn1 = Elasticsearch(hosts=[{'host': '', 'port': 0}], timeout=1000, http_auth=None)

        self.es_list = [elastic_conn, elastic_conn1]
        RecommendHandler.__init__(self)


    def start_feature(self):
        path_dir_mk = os.makedirs(self._feature_user_tar_path,exist_ok=True)

        path2 = self.download(url, path_dir_mk)  # 子类父类，继承
        tag_info_path = os.path.join(self._feature_user_tar_path, self._feature_user_file_name_untar,"tag_info.json")
        dnum_idx_path = os.path.join(self._feature_user_tar_path, self._feature_user_file_name_untar,"user_info.json")
        _tag_idx_dict = self.get_tag_idx(tag_info_path)
        _user_id_dict = self.get_dnum_idx(dnum_idx_path)

        need_to_update_env = get_es_conn("develop_env")

        self.get_user_info(self._user_profile_path, _user_id_dict, _tag_idx_dict)


class UserItemFeature(FeatureHandler):

    def __init__(self):
        FeatureHandler.__init__(self)
        self._feature_user_tar_path = os.path.join(BASE_DIR, "feature_all", self._current_time)
        self._done_file = os.path.join(self._feature_user_tar_path, f"{self._current_time}.done")
        self._feature_user_file_name = "feature_all.tar.gz"
        self._feature_user_file_name_untar = "feature_all"
        self._oss_download_url = os.path.join(self._oss_f_path, self._current_time, self._feature_user_file_name)
        self._user_profile_path = os.path.join(self._feature_user_tar_path, self._feature_user_file_name_untar,"user_profile")
        self.item_target_index = f"recommend_item_feature_{self._current_time}test"
        self._item_json_file_path = os.path.join(self._feature_user_tar_path, self._feature_user_file_name_untar, "item_info.json")
        self._target_index = f"recommend_user_feature_{self._current_time}_test_feature_all"


    def upload_es(self, es_client_target, data_info, target_index):
        timestamp = int(round(time.time() * 1000))
        bulks = []
        item_id = data_info["item_id"]
        item_id_idx = data_info["item_id_index"]
        tag_indices = data_info["tag_indices"]
        channel_idx = data_info["channel_index"]
        bulks.append({"_id": str(item_id) + "channel" + str(channel_idx), "_index": target_index, "item_id": item_id,
                      "tag_indices": tag_indices,
                      "item_id_index": item_id_idx, "channel_index": channel_idx, "status": 1, "cache": 1, "update": 0,
                      "timestamp": timestamp})

        elasticsearch.helpers.bulk(es_client_target, bulks)

    def getdata_all(self):
        with open(self._item_json_file_path, "r") as f:
            item_idx_dict_list = json.load(f)
        return item_idx_dict_list

    def start_feature(self):
        if not os.path.exists(self._feature_user_tar_path):
            self.path_dir_mk = os.makedirs(self._feature_user_tar_path)
        self.download()
        tag_info_path = os.path.join(self._feature_user_tar_path,self._feature_user_file_name_untar,"tag_info.json")
        dnum_idx_path = os.path.join(self._feature_user_tar_path,self._feature_user_file_name_untar,"user_info.json")
        _tag_idx_dict = self.get_tag_idx(tag_info_path)
        _user_id_dict = self.get_dnum_idx(dnum_idx_path)

        need_to_update_env = get_es_conn("develop_env")

        self.get_user_info(self._user_profile_path, _user_id_dict, _tag_idx_dict, need_to_update_env)

        item_idx_dict_list = self.getdata_all()
        bar_request_api = tqdm(desc="updating item", total=len(item_idx_dict_list))
        bar_request_api.set_lock(lock=Lock())
        with ThreadPoolExecutor(max_workers=8) as executor:
            for item_info in item_idx_dict_list:
                f = executor.submit(self.upload_es, need_to_update_env, item_info, self.item_target_index )
                f.add_done_callback(lambda future: bar_request_api.update())
        bar_request_api.close()

#
# def update_feature_user_day(hour="9", minute="40", run_now=False):
#     update_everyday = EveryDay()
#     scheduler = BlockingScheduler()
#     next_run_time = undefined
#     if run_now:
#         next_run_time = datetime.datetime.now()
#
#     scheduler.add_job(update_everyday.start_feature_user, "cron", hour=hour, minute=minute, next_run_time=next_run_time)
#     scheduler.start()
#
#
# def update_feature_user_item_week(day_of_week = "mon", hour="12", minute="40", run_now=False):
#     update_everyweek = EveryWeek()
#     scheduler = BlockingScheduler()
#     next_run_time = undefined
#     if run_now:
#         next_run_time = datetime.datetime.now()
#
#     scheduler.add_job(update_everyweek.start_feature_user_item, "cron", day_of_week =day_of_week , hour=hour, minute=minute, next_run_time=next_run_time)
#     scheduler.start()


@cli.command()
@click.option(help="更新频率", default="everyday")
def pipeline_():
    handler = UserFeature()
    handler.start_feature()

@cli.command()
@click.option(help="更新频率", default="everyday")
def pipeline():
    update_feature_user_item_week(run_now=True)



if __name__ == "__main__":
    cli()
