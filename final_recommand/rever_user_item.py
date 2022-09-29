import shutil

import elasticsearch
from elasticsearch import Elasticsearch, helpers
import pypinyin
from concurrent.futures.thread import ThreadPoolExecutor
# 获取dnum和其对应的dnum_index
import time
import json
import oss2
from retry import retry
import os
import gzip
import os
from  tqdm import tqdm
import pandas as pd
from apscheduler.util import undefined
import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
import csv
from ast import literal_eval
import tarfile

def get_dnum_idx():
    user_id_dict = {}
    final_dnum_list = []
    with open("./download_file/user_info.json", "r") as f:
        dnum_list = json.load(f)
        print(dnum_list[0])
        for dnum_dict in dnum_list:
            user_id = dnum_dict["dnum"]
            user_id_index = dnum_dict["idx"]

            user_id_dict[user_id] = user_id_index
            final_dnum_list.append(user_id)

    print(len(final_dnum_list))
    # print(len(set(dnum_list)))
    return user_id_dict, final_dnum_list


def get_tag_idx():
    # 生成tag和idx对应的字典
    with open("./download_file/tag_info.json", "r", encoding="utf-8") as f:
        tag_idx_dict = json.load(f)
    return tag_idx_dict


def get_user_tag_idx():
    dir_list = os.listdir("./download_file/user_profile/")
    user_info_list = []
    for parquet_file in dir_list:
        if parquet_file != "_SUCCESS":
            frame = pd.read_parquet(f"./download_file/user_profile/{parquet_file}",
                                    engine='pyarrow')
            # print(frame.head(3))
            df_as_json = frame.to_json(orient='records', lines=True)
            for json_document in df_as_json.split('\n'):
                _source = json.loads(json_document)
                user_info_list.append(_source)
                if len(user_info_list) == 10:
                    break
    return user_info_list

# 生成user_features索引
# def get_user_info(es_client,es_client_target,user_tags,user_tag_index,tag_idx,target_index,dnum_idx,dnum_list):

def get_user_info(dnum_idx, target_index, tag_idx_dict, json_document, es_client_target):
    # bulks = []
    # tag_idx_dict = {}
    channel_idx = {'纪录片': 1, '电影': 2, '综艺': 3, '电视剧': 4, '少儿': 5, '动漫': 6, 'UNK': 0}


    timestamp = int(round(time.time() * 1000))

    bulks = []
    dnum = json_document["dnum"]
    user_channel = json_document["channel"]
    user_recall_tags = json_document["tags"]
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
        user_idx = dnum_idx[dnum]
        if len(user_rank_tags_list_final) >= 20:

            for tag in user_rank_tags_list_final[1:21]:
                # kkk += 1
                if rank_tag_string == "":
                    rank_tag_string = str(tag_idx_dict[tag])
                else:
                    rank_tag_string = rank_tag_string + " " + str(tag_idx_dict[tag])

        else:
            while len(user_rank_tags_list_final) < 20:
                user_rank_tags_list_final.append("PAD")
            for tag in user_rank_tags_list_final:
                if rank_tag_string == "":
                    rank_tag_string = str(tag_idx_dict[tag])
                else:
                    rank_tag_string = rank_tag_string + " " + str(tag_idx_dict[tag])

        bulks.append(
            {"_id": user_channel_pinyin + str(dnum), "_index": target_index, "user_id": dnum,
             "channel": user_channel,
             "user_id_index": user_idx, "channel_index": channel_idx[f"{user_channel}"],
             "recall_tags": user_recall_tags, "rank_tag_indices": rank_tag_string, "timestamp": timestamp})
        # print(bulks)
        elasticsearch.helpers.bulk(es_client_target, bulks)
    except  : pass



@retry(tries=3)
def download():
    auth = oss2.Auth('', '')
    bucket = oss2.Bucket(auth, 'http://xx', 'online')
    # current_time = (datetime.datetime.now().date() - datetime.timedelta(days=1)).strftime('%Y%m%d')
    current_time = "20211125"
    f_path = "//1.0.0/"
    file_path = f_path + current_time + '.done'
    download_file_name = []
    # print(bucket.list_objects(prefix=file_path))
    exist = bucket.object_exists(file_path)
    if exist:
        bucket.get_object_to_file(f'{file_path}', f"./download/{current_time}.done")
        with open(f"./download/{current_time}.done","r") as file_done:
            content = file_done.readlines()
            for file in content:
                download_file_name.append(file.strip('\n'))
            print(content)
        print(download_file_name)

        for file_name in download_file_name:
            url = f_path + current_time +"/"+ file_name
            print(url)
            try:
                bucket.get_object_to_file(f'{url}', f"./download/{file_name}")
                tar = tarfile.open(f"./download/{file_name}", 'r')
                # print(tar)
                tar.extractall(path='./download_file')
                tar.close()
                with open(f"{current_time}.done","w",encoding="itf-8") as f:
                    f.write(f"{file_name}")
            except : pass


def untar():
    del_list = os.listdir("./download_file")
    for f in del_list:
        f_path = os.path.join("./download_file", f)
        if os.path.isfile(f_path):
            os.remove(f_path)
        elif os.path.isdir(f_path):
            shutil.rmtree(f_path)

    file_name = ["download/item_bayes_20211124.tar.gz", "download/user_profile_20211124.tar.gz",
                 "download/rank_features_20211125.tar.gz"]
    for file in file_name:
        tar = tarfile.open(f"./{file}",'r')
        # print(tar)
        tar.extractall(path='./download_file')
        tar.close()


def upload_es(es_client_target,data_info,target_index):

    timestamp = int(round(time.time() * 1000))
    bulks = []
    # for data_info in upload_data:
    item_id = data_info["item_id"]
    item_id_idx = data_info["item_id_index"]
    tag_indices = data_info["tag_indices"]
    channel_idx = data_info["channel_index"]
    bulks.append({"_id": str(item_id)+"channel"+str(channel_idx), "_index": target_index, "item_id": item_id,
                  "tag_indices": tag_indices,
                  "item_id_index": item_id_idx, "channel_index": channel_idx,"status":1,"cache":1,"update":0,"timestamp":timestamp})

    elasticsearch.helpers.bulk(es_client_target, bulks)

def getdata_all():
    # item_idx_dict = []
    with open("./download_file/item_info.json", "r") as f:
        item_idx_dict_list = json.load(f)
        # item_idx_dict.append(item_json)
    return item_idx_dict_list


def pipeline():

    if_update = download()
    if if_update:
        es_client_target = Elasticsearch(hosts=[{"host": "", "port": 8710}], timeout=1000, http_auth=None)
        assert es_client_target.ping(), "搜索服务连接失败！"
        target_index = "recommend_user_features_v10"

        # untar()
        dnum_idx = get_dnum_idx()
        tag_idx_dict = get_tag_idx()
        user_info_list = get_user_tag_idx()
        # total_num = len()
        # def upload(future):

        bar = tqdm(desc="uploading",total=len(user_info_list))
        with ThreadPoolExecutor(max_workers=12) as executor:
            for user_info in user_info_list:
                f = executor.submit(get_user_info,dnum_idx[0], target_index, tag_idx_dict,user_info,es_client_target)
                f.add_done_callback(lambda future: bar.update())
        bar.close()


        upload_data = getdata_all()
        target_index = "recommend_item_features_v10"
        # upload_es(es_client_target, upload_data, target_index)
        bar = tqdm(desc="uploading", total=len(upload_data))
        with ThreadPoolExecutor(max_workers=12) as exceutor:
            for item_info in upload_data:
                f = exceutor.submit(upload_es, es_client_target, item_info, target_index)
                f.add_done_callback(lambda future: bar.update())
        bar.close()

def start(hour = "10",minute = "30", run_now = False):
    scheduler = BlockingScheduler()
    next_run_time = undefined
    if run_now:
        next_run_time = datetime.datetime.now()

    scheduler.add_job(pipeline, "cron", hour=hour, minute=minute, next_run_time = next_run_time)
    scheduler.start()



if __name__ == "__main__":
    start(run_now=True)

