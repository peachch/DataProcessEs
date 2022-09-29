#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import tensorflow as tf
import numpy as np
import use_model_calculate_emb.tokenization
from use_model_calculate_emb import tokenization
import codecs
import json
from tqdm import tqdm
from use_model_calculate_emb.sum_media_info import extract
from setting import VOCAB_FILE,FROZEN_MODEL,INPUT1_NODE,MASK1_NODE,OUTPUT1_NODE,INPUT2_NODE,MASK2_NODE,OUTPUT2_NODE,MAX_SEQ_LEN,N2_FILE,VERSION
from functools import wraps
from concurrent.futures.thread import ThreadPoolExecutor

# VERSION = 'v0.1.0.9'

def get_tensor_graph(model_pb_file):
    with tf.io.gfile.GFile(model_pb_file, "rb") as f:
        graph_o = tf.compat.v1.GraphDef()
        graph_o.ParseFromString(f.read())
    with tf.Graph().as_default() as G:
        tf.import_graph_def(graph_o,
                            input_map=None,
                            return_elements=None,
                            name='',
                            op_dict=None,
                            producer_op_list=None)
    return G


def text_to_id(text, max_seq_len, tokenizer):
    # origin = text
    # text = tokenization.convert_to_unicode(text)
    # token = tokenizer.tokenize(text)
    #
    # tokens = token[:max_seq_len]
    #
    # input_ids = tokenizer.convert_tokens_to_ids(tokens)
    # input_mask = [1] * len(input_ids)
    #
    # # if sum(input_mask) == 0:
    # #     print('mask becomes 0, input text: ', origin)
    #
    # return input_ids, input_mask
    origin = text
    text = tokenization.convert_to_unicode(text)
    token_text = tokenizer.tokenize(text)
    # token = list(text)
    if len(token_text) > max_seq_len - 2:
        token_text = token_text[:max_seq_len - 2]

    tokens = []
    tokens.append("[CLS]")
    # print(tokens)
    for token in token_text:
        tokens.append(token)
    tokens.append("[SEP]")
    # TODO: non-chinese vocab
    if "[UNK]" in tokens:
        pass

    # format input data

    input_ids = tokenizer.convert_tokens_to_ids(tokens)
    input_mask = [1] * len(input_ids)
    while len(input_ids) < max_seq_len:
        input_ids.append(0)
        input_mask.append(0)
    assert len(input_ids) == max_seq_len
    assert len(input_mask) == max_seq_len

    if sum(input_mask) == 0:
        print('original sentence seems to be empty, input text: ' + repr(origin))
    return input_ids, input_mask

def memorize(function):
        memo = {}
        @wraps(function)
        def wrapper(*args):
            if not args in memo:
                rv = function(*args)
                memo[args] = rv
                return rv
            else:
                return memo[args]
        return wrapper


class DocEmbedding(object):

    def __init__(self, path='./'):
        self.graph = get_tensor_graph(path + FROZEN_MODEL)
        config = tf.compat.v1.ConfigProto(allow_soft_placement=True, gpu_options=tf.compat.v1.GPUOptions(allow_growth=True))
        self.sess = tf.compat.v1.Session(graph=self.graph, config=config)

        self.input2_node = self.sess.graph.get_tensor_by_name(
            INPUT2_NODE)
        self.mask2 = self.sess.graph.get_tensor_by_name(
            MASK2_NODE)
        self.output2_node = self.sess.graph.get_tensor_by_name(
            OUTPUT2_NODE)

        self.tokenizer = tokenization.FullTokenizer(
            vocab_file=path + VOCAB_FILE, do_lower_case=True)

    @memorize
    def get_keywords(self,single_data_dict):
        keywords = single_data_dict['title']
        extracted_dict = {'aid': single_data_dict['aid'],
                          'vendor': single_data_dict['vendor']}
        print("sssssssss",keywords)
        return keywords,extracted_dict

    @memorize
    def get_embed(self,keywords):
        inp2, m2 = text_to_id(keywords, MAX_SEQ_LEN, self.tokenizer)
        n2 = self.sess.run(self.output2_node, feed_dict={self.input2_node: [inp2],
                                                         self.mask2: [m2]})
        return n2

    def return_embed(self,extracted_dict,n2):
        extracted_dict.update({'vector': n2[0].tolist()})
        return extracted_dict

    def get_embed_pipeline(self,data_line,w):
        for single_data_line in data_line:

            single_data_dict = json.loads(single_data_line)
            print(type(single_data_dict))
            # return_params = self.get_keywords(single_data_dict)
            keywords = single_data_dict['title']
            extracted_dict = {'aid': single_data_dict['aid'],
                              'vendor': single_data_dict['vendor']}
            n2 = self.get_embed(keywords)
            final_re = self.return_embed(extracted_dict, n2)
            w.write(json.dumps(final_re, ensure_ascii=False) + '\n')

    def embed(self,data_json_file,save_file):
        with codecs.open(data_json_file, 'r', encoding='utf-8') as f, codecs.open(save_file, 'a', encoding='utf-8') as w:
            all_file = f.read().split('\n')[:-1]
            print("看下这里",len(all_file))
            # print(all_file)
            with ThreadPoolExecutor(max_workers=12) as executor:
                for line in range(0,len(all_file)+1,500):
                    sub_line = all_file[line:line+500]
                    f = executor.submit(self.get_embed_pipeline,sub_line,w)




        #w.write(json.dumps(extracted_dict, ensure_ascii=False) + '\n')



class QueryEmbedding(object):
    def __init__(self, path='./'):
        self.graph = get_tensor_graph(path + FROZEN_MODEL)
        config = tf.compat.v1.ConfigProto(allow_soft_placement=True,
                                          gpu_options=tf.compat.v1.GPUOptions(allow_growth=True))
        self.sess = tf.compat.v1.Session(graph=self.graph, config=config)
        self.input1_node = self.sess.graph.get_tensor_by_name(
            INPUT1_NODE)
        self.mask1 = self.sess.graph.get_tensor_by_name(
            MASK1_NODE)
        self.output1_node = self.sess.graph.get_tensor_by_name(
            OUTPUT1_NODE)
        self.tokenizer = tokenization.FullTokenizer(
            vocab_file=path + VOCAB_FILE, do_lower_case=True)

    def embed(self, query):
        query_id, query_mask = text_to_id(query, MAX_SEQ_LEN, self.tokenizer)

        norm1 = self.sess.run(self.output1_node,
                              feed_dict={
                                  self.input1_node: [query_id],
                                  self.mask1: [query_mask],
                              })[0]

        return norm1



