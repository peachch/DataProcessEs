from use_model_calculate_emb.pb_model import DocEmbedding
from use_model_calculate_emb.pb_model import QueryEmbedding
from upload_to_index.uploadDocEmbedding_knn import UploadDocToIndex
from setting import HOST_NAME,PORT,SEARCH_ALBUM_INDEX_NAME,ES_CLIENT,SHORT_QUERY_EMBEDDING_FROM_SEARCH_ALBUM_INDEX,POSITIVE_QUERY_EMBEDDING_FROM_SEARCH_ALBUM_INDEX,DOC_RAW_FILE,DOC_VECTOR_FILE,ES_LOG_NAME,QUERY_VECTOR_FILE,\
GET_ALL_QUERY_FROM_ES,SIM_DICT_FILE,SIM_WEI_JSON_FILE
from elasticsearch import Elasticsearch , helpers
from upload_to_index.uploadQueryEmbedding import UploadQueryToIndex
from upload_to_index.upload_title_brief_char import SimKeywords
import elasticsearch
import json
import datetime
import time
import os.path
import threading,time
import argparse
from concurrent.futures.thread import ThreadPoolExecutor
from use_model_calculate_emb.wei import get_keywords
import tensorflow as tf
def get_all_query_file_to_es_log(get_query_from_es):
    es_query_client = Elasticsearch([{'host': "00.00.00", 'port': "0000"}], timeout=1000, http_auth=None)
    assert ES_CLIENT.ping(), "搜索服务连接失败！{120.76.173.207:9200}"
    model = QueryEmbedding()
    i = 0
    response =elasticsearch.helpers.scan(es_query_client, get_query_from_es, scroll="15m", size=1000,
                           index=ES_LOG_NAME)

    response_dict = {}
    if os.path.isfile(QUERY_VECTOR_FILE):
        vector_file_raw = open(QUERY_VECTOR_FILE,"r",encoding="utf-8")
        vector_file_content = [line.strip("\n") for line in vector_file_raw.readlines()]
        for vector in vector_file_content:
            vector_list = vector.split('\t')
            response_dict[vector_list[0]]=vector_list[1]
        vector_file_raw.close()

    vector_file = open(QUERY_VECTOR_FILE, "a", newline='',encoding="utf-8")
    vector_need_to_upload = []
    for res_dict in response:
        #print("here")
        i += 1
        print(i)
        res_str = res_dict["_source"]["queryText"]
        if i == 200:
            break
        if not response_dict.get(res_str):
            norm1 = list(model.embed(res_str))
            response_dict[res_str] = norm1
            vector_file.write(str(res_str) + '\t' + str(norm1)+'\n')
            vector_need_to_upload.append([str(res_str),str(norm1)])
            continue
        #print(vector_need_to_upload)
    return vector_need_to_upload

def dssm_vector():
    res_str = "慱朗手动料理机"
    corpus_str = "中国风冬季唐装男青年棉袄外套羊羔毛加绒棉衣盘扣中式中山装复古"
    corpus_file = open("dssm_corput_embedding", "a", newline='', encoding="utf-8")
    res_file = open("dssm_corput_embedding", "a", newline='', encoding="utf-8")
    model = QueryEmbedding()

    norm1 = model.embed(corpus_str)
    print("ssssssssssssssssssssss", norm1)
    from sklearn.decomposition import PCA
    pca = PCA(n_components=128)  # 直接与变量个数相同的主成分
    pca.fit(norm1)
    new_data = pca.fit_transform(norm1)
    print(new_data)

    corpus_file.write(str(corpus_str) + '\t' + str(norm1) + '\n')

    norm2 = model.embed(res_str)
    res_file.write(str(res_str) + '\t' + str(norm2) + '\n')
    print(list(norm1))
    query_norm = tf.sqrt(tf.reduce_sum(tf.square(list(norm1))))
    # doc_norm = sqrt(sum(each x^2))
    doc_norm = tf.sqrt(tf.reduce_sum(tf.square(norm2)))

    # 内积
    prod = tf.reduce_sum(tf.multiply(
        norm1, norm2))
    # 模相乘
    mul = tf.multiply(query_norm, doc_norm)
    # cos_sim_raw = query * doc / (||query|| * ||doc||)
    # cos_sim_raw = tf.truediv(prod, tf.multiply(query_norm, doc_norm))
    cos_sim_raw = tf.divide(prod, mul)
    predict_prob = tf.sigmoid(cos_sim_raw)
    print("result: ", predict_prob, cos_sim_raw)

def filter_data_from_es(QUERY_POSTIVE,QUERY_SHORTVIDEO):
    """电影channel短视频"""
    # query_shortvideo = {
    #         "query": {
    #             "bool": {
    #                 "must": [
    #                     {"terms": {"vendor": ["13","15"]}},
    #                     {"terms": {"type": ["2","3","4","5"]}},
    #                     {"term": {"status": "1"}},
    #                     {"term": {"channelid.keyword": "001"}},
    #                 ]}}
    #             }
    # query_positive = {
    #     "query": {
    #         "bool": {
    #             "must": [
    #                 {"terms": {"vendor": ["13", "15"]}},
    #                 {"term": {"type": "1"}},
    #                 {"term": {"status": "1"}},
    #                 {"term": {"channelid.keyword": ["001","002","003","004","005","023"]}},
    #             ]}}
    # }
    query_all = [json.dumps(QUERY_SHORTVIDEO,ensure_ascii=False)]
    print(query_all)
    i = 0
    k = 0
    #global DOC_RAW_FILE,DOC_VECTOR_FILE
    doc_raw_path = DOC_RAW_FILE
    doc_vector_path = DOC_VECTOR_FILE
    # 重新开始计算，将原始的数据删除掉
    if os.path.isfile(doc_raw_path):
        os.remove(doc_raw_path)
        print(doc_raw_path + " was removed!")

    if os.path.isfile(doc_vector_path):
        os.remove(doc_vector_path)
        print(doc_vector_path + " was removed!")

    for query in query_all:
        query = json.loads(query)
        response =elasticsearch.helpers.scan(ES_CLIENT, query, scroll="15m", size=1000,
                               index=SEARCH_ALBUM_INDEX_NAME)
        k+=1
        print("请求到第几个query",k)
        print(response)
        for res_dict in response:
            i+=1
            #print(res_dict)
            res_dict_new = res_dict["_source"]
            jsObj_shortvide = json.dumps(res_dict_new,ensure_ascii=False)
            doc_embedding_raw_file = open(DOC_RAW_FILE,"a")
            doc_embedding_raw_file.write(jsObj_shortvide+'\n')
            doc_embedding_raw_file.close()
            print(i)
            if i == 100:
                break

class AddUpdate(object):
    def __init__(self):
        self.current_time = datetime.datetime.now().date() - datetime.timedelta(days=100)
        self.curTime = self.current_time.strftime("%Y-%m-%d")
        self.execF = False
        self.ncount = 0
    def execTask(self):
        get_add_qeury_from_es = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"slots.name.keyword": "Keyword"}},
                        {"term": {"queryDate": self.curTime}}
                    ]}}
        }
        vector_need_to_upload = get_all_query_file_to_es_log(get_add_qeury_from_es)
        print(vector_need_to_upload)
        # query_emb = UploadQueryToIndex()
        # query_emb.buildInedx()
        # query_emb.thread_deal(vector_need_to_upload)

    def timerTask(self):
        if self.execF is False:
            self.execTask()
            self.execF = True
        else:
            current_time = datetime.datetime.now().date() - datetime.timedelta(days=100)
            desTime = current_time.strftime("%Y-%m-%d")
            if desTime > self.curTime:
                self.execF =False
                self.curTime = desTime
        self.ncount = self.ncount + 1
        timer = threading.Timer(5, self.timerTask)
        timer.start()
        print("定时器执行%d次" % (self.ncount))

if __name__ == '__main__':
    """阿里云搜索比赛"""
    dssm_vector()


    # """get query_embedding file"""
    # """计算得到对应queryText的vector，并维护文件，这里适合全量操作包括第一次全量和更改模型之后（在操作时需要删除
    # query_vector_file_from_es.json文件、和删除此索引）、误删除索引之后的修复、当需要上传新的数据，直接运行，
    # 会直接进行去重处理，将没有处理过的vector新增到es"""
    # parser = argparse.ArgumentParser(description='manual to this script')
    # parser.add_argument('--mode', help="设置query更新的方式，add为增量更新，all为全量更新",type=str, default="add")
    # args = parser.parse_args()
    # vector_need_to_upload = []
    # """ python main.py --mode="all" """
    # if args.mode == "query_all":
    #     """上传query信息到es，id通过计算得来，保证相同的query对应的id相同"""
    #     print("update all data")
    #     vector_need_to_upload = get_all_query_file_to_es_log(GET_ALL_QUERY_FROM_ES)
    #     query_emb = UploadQueryToIndex()
    #     query_emb.buildInedx()
    #     query_emb.thread_deal(vector_need_to_upload)
    # """更新query，设置一个一天的定时器，每一天运行一次去更新。"""
    # if args.mode == "query_add":
    #     print("add data")
    #     app_update = AddUpdate()
    #     timer = threading.Timer(10, app_update.timerTask)
    #     timer.start()
    #
    # if args.mode == "doc_embedding_all":
    #     top_k = 30
    #     """get all doc embedding data from es"""
    #     filter_data_from_es(SHORT_QUERY_EMBEDDING_FROM_SEARCH_ALBUM_INDEX,
    #                         SHORT_QUERY_EMBEDDING_FROM_SEARCH_ALBUM_INDEX)
    #     """get doc_embedding file"""
    #     doc = DocEmbedding()
    #     doc.embed(data_json_file=DOC_RAW_FILE, save_file=DOC_VECTOR_FILE)
    #     """upload doc_embedding file to es"""
    #     upload = UploadDocToIndex()
    #     upload.rebuild_index()
    #     upload.uplodaDocEmb()
    # if args.mode == "sim_keywords_all":
    #     #aaa = get_keywords(data_json_file=DOC_RAW_FILE, save_file=SIM_WEI_JSON_FILE,sim_dict_file=SIM_DICT_FILE)
    #     sim_keywords = SimKeywords()
    #     sim_keywords.uploadsimkeywrods()
    #
    #
    #
    #
