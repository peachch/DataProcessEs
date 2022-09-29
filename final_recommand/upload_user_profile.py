# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import json
from elasticsearch import Elasticsearch
from elasticsearch import helpers

#d1=pd.read_json("search_album.json")
#frame=pd.read_parquet("/data/yjy/tym/data/user_profile/tag_profile_1000", engine='pyarrow')
frame=pd.read_parquet("/data/yjy/tym/data/user_profile/tag_profile_10", engine='pyarrow')
print(frame.head(3))

index_ = 'user_tag_profile'
type_ = 'doc'
try:
    es = Elasticsearch([{"host": "47.106.214.27", "port": 8710}])
    print("database connet successfully")
    df_as_json = frame.to_json(orient='records', lines=True)
    bulk_data = []

    i = 0
    for json_document in df_as_json.split('\n'):

        _source = json.loads(json_document)
        _source['_index'] = index_
        #bulk_data.append({"index": {
        #bulk_data.append({"index": {
        #bulk_data.append({
        #                '_index': index_,
        #                "dnum": _source["dnum"],
        #                "channel": _source["channel"],
        #                "tags": _source["tags"]
        #})
        #print(bulk_data)
        bulk_data.append(_source)
        #bulk_data.append({json.loads(json_document)})
        #print(bulk_data)
        # 一次bulk request包含1000条数据
        if len(bulk_data) > 1000:
            #es.bulk(bulk_data)
            helpers.bulk(es, bulk_data, request_timeout=3000) 
            bulk_data = []
            print('bulk insert into es counter:' + str(i))
            #if i > 2:
            #    break
        i = i + 1
    helpers.bulk(es, bulk_data, request_timeout=300) 
    #es.bulk(bulk_data)
    #es.bulk(body=bulk_data, index= index_)
    print('database bulk insert  successfully')

except Exception as e:
    print(e)
