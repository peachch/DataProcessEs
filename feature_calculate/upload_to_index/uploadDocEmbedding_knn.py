#coding=UTF-8
import json
import datetime
import sys

import elasticsearch
import numpy as np
from elasticsearch import Elasticsearch , helpers
from setting import SEARCH_ALBUM_INDEX_NAME,PORT,HOST_NAME,KNN_DOC_EMBEDDING_MAPPING,KNN_DOC_EMBEDDING_SETTINGS,ES_CLIENT,DOC_VECTOR_FILE
import threading
from tqdm import tqdm
from concurrent.futures.thread import ThreadPoolExecutor
from threading import Lock

lock = Lock()

def readtxtfile(fname, encoding='utf-8'):
    try:
        with open(fname, 'r', encoding=encoding) as f:
            data = f.read()
        return data
    except Exception as e:
        return ''

class UploadDocToIndex(object):
    def __init__(self,es_client=ES_CLIENT):
        self.es_client = es_client
    def rebuild_index(self):
        # 判断索引是否存在，若不存在则创建
        if ES_CLIENT.indices.exists(index=SEARCH_ALBUM_INDEX_NAME) != True:
            # 这里需要先关闭 才可以进行put_settings
            print('ready to rebuild_index')
            try:
                status_code = self.es_client.indices.close(index=SEARCH_ALBUM_INDEX_NAME)
                print("close yet",status_code)
            except Exception as e:
                print(repr(e))

            status_code = self.es_client.indices.put_settings(index=SEARCH_ALBUM_INDEX_NAME, body=KNN_DOC_EMBEDDING_SETTINGS)
            print("change settings of knn", status_code)

            status_code = self.es_client.indices.open(index=SEARCH_ALBUM_INDEX_NAME)
            print("open again yet",status_code)

            status_code = self.es_client.indices.put_mapping(index=SEARCH_ALBUM_INDEX_NAME,body=KNN_DOC_EMBEDDING_MAPPING)
            print("change mapping of knn",status_code)
            print("Build new index!!!")
        print("No need to rebuild index!")


    def uplodaDocEmb(self):
        sentences = readtxtfile(DOC_VECTOR_FILE).splitlines()
        length = len(sentences)
        print('vector_file 总记录数:', length)
        start_time = datetime.datetime.now()
        print(sentences)
        def uppload(sub_sentences):
            ACTIONS = []
            for line in sub_sentences:
                json_data = json.loads(line)
                ACTIONS.append({
                    "_index": SEARCH_ALBUM_INDEX_NAME,
                    '_op_type': 'update',
                    "_id": str(json_data['vendor']+json_data['aid']),
                    "_source": {"doc": {"knn_data_dense_vector": json_data['vector']}}
                })
                # k += 1
            # print(len(ACTIONS))
            return ACTIONS

        imported_dnums_record = []
        def on_done(future):
            # print("这个线程对应的结果数",len(future.result()))
            with lock:
                elasticsearch.helpers.bulk(self.es_client, future.result(), index=SEARCH_ALBUM_INDEX_NAME,
                                           raise_on_error=False)
                imported_dnums_record.extend(future.result())
                imported_count = len(imported_dnums_record)
                process_rate = int(100 * (imported_count / sentences_count))
                # 打印进度
                print(
                    f"Importing data: {process_rate}%|{'█' * process_rate + ' ' * (100 - process_rate)}| {imported_count}/{sentences_count}",
                    end="\r" if process_rate < 100 else "\n")


        sentences_count = len(sentences)
        with ThreadPoolExecutor(max_workers=12) as executor:
            for index in range(0,sentences_count+1,500):
                sub_sentences = sentences[index:index+500]
                f = executor.submit(uppload, sub_sentences)
                f.add_done_callback(on_done)
            executor.shutdown()
        # print("需要上传的actions总数", len(ACTIONS))


        end_time = datetime.datetime.now()
        time_cost = end_time - start_time
        print(" 耗时：" + str(int(time_cost.total_seconds()*1000)) + "ms")


# if __name__ == "__main__":
#     es_client = Elasticsearch([{'host': HOST_NAME, 'port': PORT}], timeout=120)
#     rebuild_index(es_client)
#     # uplodaDocEmb(es_client)
