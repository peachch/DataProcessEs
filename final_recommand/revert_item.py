import elasticsearch
from elasticsearch import Elasticsearch,helpers
import time
import json
from tqdm import tqdm
from concurrent.futures.thread import ThreadPoolExecutor
# 获取item和其对应的item_index
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
    with open("./download_file/rank_features/item_features_with_item_id_idx.json", "r") as f:
        item_idx_dict_list = json.load(f)
        # item_idx_dict.append(item_json)
    return item_idx_dict_list


def pipeline():
    es_client_target = Elasticsearch(hosts=[{"host": "", "port": 8700}], timeout=1000, http_auth=None)
    assert es_client_target.ping(), "搜索服务连接失败！"

    upload_data = getdata_all()
    target_index = "recommend_item_features_v3"
    # upload_es(es_client_target, upload_data, target_index)
    bar = tqdm(desc="uploading" ,total=len(upload_data))
    with ThreadPoolExecutor(max_workers=12) as exceutor:
        for item_info in upload_data:
            f = exceutor.submit(upload_es,es_client_target,item_info,target_index)
            f.add_done_callback(lambda future: bar.update())
    bar.close()


if __name__ == "__main__":
    pipeline()
