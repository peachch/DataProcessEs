#coding=UTF-8
import json
import datetime
import sys
import numpy as np

from elasticsearch import Elasticsearch , helpers
import threading
from setting import ES_CLIENT,SEARCH_ALBUM_INDEX_NAME,SIM_WEI_JSON_FILE,SIM_KEYWORDS_MAPPING
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

class SimKeywords(object):
    def __init__(self):
        self.es_client = ES_CLIENT
    def rebuildindex(self):

        status_code = self.es_client.indices.put_mapping(index=SEARCH_ALBUM_INDEX_NAME,body=SIM_KEYWORDS_MAPPING)
        print("change mapping of knn",status_code)
        print("Build new index!!!")
        print("No need to rebuild index!")
    def uploadsimkeywrods(self):
        sentences = readtxtfile(SIM_WEI_JSON_FILE).splitlines()
        length = len(sentences)
        print(' keyword_title_brief_char_file 总记录数:', len(sentences))
        start_time = datetime.datetime.now()


        actions = []
        for index in range(len(sentences)):
           # print(sentences[index])
            json_data = json.loads(sentences[index])
            # print(json_data)
            updateBody = {
                "script": {
                    "source": "ctx._source['sim_keywords'] = params['sim_keywords']",
                    "lang": "painless",
                    "params": {"sim_keywords": json_data['sim_keywords'],
                               }
                },
                "query": {
                    "bool": {
                        "must": [{
                            "term": {
                                "vendor.keyword": json_data['vendor']
                            }},
                            {
                            "term": {
                                "aid.keyword": json_data['aid']
                            }}
                        ]
                    }
                }
            }
            ES_CLIENT.update_by_query(index=SEARCH_ALBUM_INDEX_NAME, body=updateBody)

        end_time = datetime.datetime.now()
        time_cost = end_time - start_time
        print(" 耗时：" + str(int(time_cost.total_seconds()*1000)) + "ms")

