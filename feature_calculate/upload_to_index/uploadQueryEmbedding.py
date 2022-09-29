#coding=UTF-8
import json
import datetime
import sys
import numpy as np
import hashlib
from setting import ES_CLIENT,QUERY_INDEX_NAME,QUERY_INDEX_SETTINGS,QUERY_VECTOR_FILE
from elasticsearch import Elasticsearch , helpers
import threading
from concurrent.futures.thread import ThreadPoolExecutor
import time

def double2Float(doubleVector):
    floatVector = []
    for i in doubleVector:
        floatVector.append(float('%.6f' % i))
    return floatVector
def readtxtfile(fname, encoding='utf-8'):
    try:
        with open(fname, 'r', encoding=encoding) as f:
            data = f.read()
        return data
    except Exception as e:
        return ''

def str2array (txt):
    ret = []
    try:
        ret = json.loads(txt)
    except :
        pass
        print('Error in str2array')
    return np.array(ret)


class UploadQueryToIndex(object):

    def buildInedx(self):
        # 判断索引是否存在，若不存在则创建
        if ES_CLIENT.indices.exists(index=QUERY_INDEX_NAME) != True:
            ES_CLIENT.indices.create(index=QUERY_INDEX_NAME, body=QUERY_INDEX_SETTINGS)
            print("Build new index")

    def uplodaQueryEmb(self,lines):
        print("here")
        #print(lines)
        chunk_len = 1500
        num = 1
        start_time = datetime.datetime.now()
        timestamp = int(round(time.time() * 1000))
        length = len(lines)
        actions = []
        k = 0
        for index in range(len(lines)):
            line = lines[index]
            # print(line)
            action = {
                "_index": QUERY_INDEX_NAME,
                "_id": hashlib.blake2b(line[0].encode('utf-8')).hexdigest(),
                "_source": {
                    "type":"semantic_search",
                    "slot_value": line[0],
                    "embedding": str2array(line[1]).tolist(),
                    "status":1,
                    "timestamp": timestamp
                }
            }

            actions.append(action)
            k+=1
            if num % 500 == 0:
                a = helpers.bulk(ES_CLIENT, actions, request_timeout=120)
                actions.clear()
            elif num == length:
                a = helpers.bulk(ES_CLIENT, actions, request_timeout=120)
                actions.clear()

            num += 1
        print("Already upload number is:",k)
        end_time = datetime.datetime.now()
        time_cost = end_time - start_time
        print(" 耗时：" + str(int(time_cost.total_seconds()*1000)) + "ms")


    def thread_deal(self,query_vector_need_to_upload):

        #lines = readtxtfile(QUERY_VECTOR_FILE).splitlines()
        print('text_file 总记录数:', len(query_vector_need_to_upload))
        with ThreadPoolExecutor(max_workers=8) as executor:
            for idx in range(0, len(query_vector_need_to_upload), 10):
                sub_querid_list = query_vector_need_to_upload[idx:idx + 10]
                #print(sub_querid_list)
                f = executor.submit(self.uplodaQueryEmb, sub_querid_list)
                f.add_done_callback(lambda f: None)
