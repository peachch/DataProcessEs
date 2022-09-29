
import hashlib
import logging
import os
import re
import time
from collections import defaultdict
from copy import copy

import settings
from common.mongo_helper import MongoHelper
from pipeline.nodes_vendor.base_node_video import extract_season_from_title
from utils.data_utils import StrClean, SplitStr, chinese2num
from consecution import Node


class BaseNode(Node):
    """打印日志、基础函数重写"""

    def process(self, item):
        processed_item = self.process_media(item)
        logging.debug(
            f"node {self.name}, data_id {processed_item.get('id', processed_item['vendor'] + processed_item['aid'])},  output {processed_item}")
        self.push(processed_item)

    def process_media(self, media_dict):
        raise NotImplementedError


class AlbumPreprocessNode(BaseNode):
    """数据预处理基础节点"""
    need_split_field_list = ["directors", "actors", "languages", "regions", "tags", "statistic_item", "awards",
                             "producers", "characters", "writer", "aliases"]

    clean_invalid_field_list = ["writer", "characters", "tags", "awards", "regions", "languages", "producers",
                                "statistic_item", "actors", "directors", "title"]

    def __init__(self, name):
        BaseNode.__init__(self, name=name)

        # 媒资预处理，添加字段转换格式等
        self.album_filed_list_type_mapping = {
            "vendor": "string",
            "aid": "string",
            "title": "string",
            "entitle": "string",
            "shorttitle": "string",
            "fullpinyin": "list",
            "simplepinyin": "list",
            "aliases": "list",
            "channel": "string",
            "channelid": "string",
            "status": "int",
            "type": "int",
            "brief": "string",
            "publishyear": "int",
            "score": "float",
            "paymode": "int",
            "vpic": "string",
            "vpicmd5": "string",
            "hpic": "string",
            "hpicmd5": "string",
            "updatetime": "string",
            "total": "int",
            "last": "string",
            "season": "string",
            "completed": "boolean",
            "regions": "list",
            "languages": "list",
            "tags": "list",
            "actors": "list",
            "directors": "list",
            "producers": "list",
            "copyright": "list",
            "characters": "list",
            "awards": "list",
            "ext": "map",
            "params": "string",
            "playurl": "string",
            "index": "int",
            "albumids": "list",
            "vid": "string",
            "duration": "int",
            'english': "string",
            "quality": "int",
            "needpay": "boolean",
            "episdoes": "string",
            "reference_time": "int",
            "writer": "list",
            "publishdate": "string"  # 初始publishdate为sting，此脚本运行完后会全部修改为int
        }

        self.album_filed_list_type_mapping_finally = {
            "vendor": "string",
            "aid": "string",
            "title": "string",
            "entitle": "string",
            "shorttitle": "string",
            "fullpinyin": "list",
            "simplepinyin": "list",
            "aliases": "list",
            "channel": "string",
            "channelid": "string",
            "status": "int",
            "type": "int",
            "brief": "string",
            "publishyear": "int",
            "score": "float",
            "paymode": "int",
            "vpic": "string",
            "vpicmd5": "string",
            "hpic": "string",
            "hpicmd5": "string",
            "updatetime": "string",
            "total": "int",
            "last": "string",
            "season": "string",
            "completed": "boolean",
            "regions": "list",
            "languages": "list",
            "tags": "list",
            "actors": "list",
            "directors": "list",
            "producers": "list",
            "copyright": "list",
            "characters": "list",
            "awards": "list",
            "ext": "map",
            "params": "string",
            "playurl": "string",
            "index": "int",
            "albumids": "list",
            "vid": "string",
            "duration": "int",
            'english': "string",
            "quality": "int",
            "needpay": "boolean",
            "episdoes": "string",
            "reference_time": "int",
            "statistic_item": "list",
            "writer": "list",
            "publishdate": "int"
        }

        # 需要预处理的字段
        self.limited_field_list = ["vendor", "ext", "aid", "channel", "title", "aliases", "brief",
                                   "status", "type", "copyright",
                                   "characters", "awards", "score", "total", "last", "completed",
                                   "season", "regions", "languages",
                                   "tags", "actors", "directors", "producers", "params", "channelid",
                                   "entitle", "fullpinyin",
                                   "simplepinyin", "paymode", "vpic", "vpicmd5", "hpic", "hpicmd5",
                                   "updatetime", "publishyear",
                                   "writer", "reference_time", "publishdate"]

    def check_data(self, preprocess_context):
        preprocess_context = self.filed_lower(preprocess_context)  # 把所有字段转小写
        preprocess_context = self.if_filed_missing_or_invaild(preprocess_context)
        preprocess_context = self.title_lower(preprocess_context)
        preprocess_context["id"] = preprocess_context["vendor"] + preprocess_context["aid"]
        return preprocess_context

    # def combine_with_old_media(self, preprocess_context):
    #     TC = TitleCombine()
    #     combine_list = ["writer", "characters", "tags"]  # 需要融合的字段
    #     compare_list = ["actors", "directors", "aid"]  # 用来匹配的条件字段
    #     old_media_source = ["mango", "iqiyi", "icntv", "mi", "snm", "wasutv", "yksy"]
    #
    #     new_media_combined_title = preprocess_context
    #     title_digest = TC.formatting_title_digest(preprocess_context["title"])
    #     for source in old_media_source:
    #         coll = f"ref_old_media_{source}"
    #         data_matched = self.mongo_conn[coll].find_one({"data_id": title_digest})
    #
    #         if data_matched:
    #             new_media_combined_title = TC.title_combine(new_media_combined_title, combine_list, compare_list,
    #                                                         data_matched, source)

    #     return new_media_combined_title

    # def combine_title_with_scrapy(self, preprocess_context, scrapy_dic, source):
    #     combine_list = ["tags"]
    #     compare_list = ["actors", "directors", "aid"]
    #     TC = TitleCombine()
    #     line_json = TC.title_combine(preprocess_context, scrapy_dic, combine_list, compare_list, source)
    #     return line_json

    def if_filed_missing_or_invaild(self, preprocess_context):  # 字段填补
        for field in self.limited_field_list:
            # 字段填补
            if field not in preprocess_context or preprocess_context[field] == None or preprocess_context[field] in [
                "-nan", "nan"]:
                try:
                    filed_type = self.album_filed_list_type_mapping[field]
                except KeyError:
                    pass
                else:
                    if filed_type == "string":
                        preprocess_context[field] = ""
                    elif filed_type == "int":
                        preprocess_context[field] = 0
                    elif filed_type == "list":
                        preprocess_context[field] = []
                    elif filed_type == "map":
                        preprocess_context[field] = {}
                    elif filed_type == "boolean":
                        preprocess_context[field] = True
                    elif filed_type == "float":
                        preprocess_context[field] = 0.0
            # 字段合法检查
            if field in ["type", "status", "index", "quality", "duration"]:
                try:
                    int(preprocess_context[field])
                except:
                    print(f"{field} 为空字段")
                else:
                    preprocess_context[field] = int(preprocess_context[field])

            elif field in ["aid", "title", "channel", "vid"]:
                if preprocess_context[field] == "":
                    print(f"{field} 为空字段")

            elif field in ["ext"]:
                ext_dic = preprocess_context[field]
                if "islast" not in ext_dic:  # 填补islast字段默认初始为ture
                    preprocess_context[field]["islast"] = True
                else:
                    if type(preprocess_context[field]["islast"]) != bool:
                        preprocess_context[field]["islast"] = True  # 布尔类型数据全部初始化为True

            elif field == "score":  # 转化所有score为float
                try:
                    preprocess_context[field] = float(preprocess_context[field])
                except ValueError:
                    preprocess_context[field] = 0.0

            elif field == "total":  # 转化所有total为int
                try:
                    preprocess_context[field] = int(preprocess_context[field])
                except ValueError:
                    preprocess_context[field] = 0

            if type(preprocess_context[field]) == list:
                if "" in preprocess_context[field]:
                    preprocess_context[field].remove("")

        # 限定字段
        for field in list(preprocess_context.keys()):
            if field not in self.limited_field_list:
                preprocess_context.pop(field)
        return preprocess_context

    @staticmethod
    def filed_lower(preprocess_context):  # 把字段名改为小写
        if "ext" in preprocess_context:
            if type(preprocess_context["ext"]) == str:
                if "true" in preprocess_context["ext"]:
                    preprocess_context["ext"] = eval(preprocess_context["ext"].replace("true", "True"))
                elif "false" in preprocess_context["ext"]:
                    preprocess_context["ext"] = eval(preprocess_context["ext"].replace("false", "False"))
                else:
                    preprocess_context["ext"] = eval(preprocess_context["ext"])

        new_preprocess_context = {}
        for field in list(preprocess_context.keys()):
            new_preprocess_context[field.lower()] = preprocess_context[field]

            if field == "ext":
                if new_preprocess_context[field] != {""} and new_preprocess_context[field] != {}:  # 将不为空的ext字段内部所有字段转小写
                    for ext_field in list(new_preprocess_context[field].keys()):
                        new_preprocess_context[field][ext_field.lower()] = preprocess_context[field][ext_field]
                        new_preprocess_context[field].pop(ext_field)
                else:
                    pass
        return new_preprocess_context

    @staticmethod
    def title_lower(preprocess_context):  # title改为小写
        preprocess_context["title"] = preprocess_context["title"].lower()
        return preprocess_context

    def params_field_for_combine(self, preprocess_context, compare_field_list):
        SC = StrClean()
        SS = SplitStr()

        # 对于数据融合需要用到的字段预先清洗处理
        for compare_field in compare_field_list:
            compare_field_value = preprocess_context[compare_field]
            # 获取字段类型
            compare_field_type = self.album_filed_list_type_mapping[compare_field]
            if compare_field_type == "list":
                new_compare_field_value = []
                # 字段拆分
                for one_compare_field_value in compare_field_value:
                    one_split_compare_field_list = SS.split_str(one_compare_field_value)
                    new_compare_field_value = new_compare_field_value + one_split_compare_field_list
                    new_compare_field_value = list(set(new_compare_field_value))
                # 字段清洗
                for index, new_value in enumerate(new_compare_field_value):
                    # 删除括号内的内容
                    new_value = SC.if_brackets_in_str(new_value)
                    # 只保留中英文和数字
                    new_value = SC.clean_sign(new_value)

                    new_compare_field_value[index] = new_value
                preprocess_context[compare_field] = new_compare_field_value

            elif compare_field_type == "string":
                # 字段拆分
                new_compare_field_value = SS.split_str(compare_field_value)
                # 字段清洗
                for index, new_value in enumerate(new_compare_field_value):
                    new_value = SC.if_brackets_in_str(new_value)
                    new_value = SC.clean_sign(new_value)
                    new_compare_field_value[index] = new_value
                new_compare_field_value = list(set(new_compare_field_value))
                preprocess_context[compare_field] = new_compare_field_value
        return preprocess_context

    @staticmethod
    def correct_actors(dic):
        actors_list = dic["actors"]
        while "" in actors_list:
            actors_list.remove("")
        dic["actors"] = actors_list
        return dic

    @staticmethod
    def correct_entitle(dic):
        dic["entitle"].replace("暂无", "")
        return dic

    @staticmethod
    def correct_languages(dic):
        languages_mapping = {
            "国语": "中文",
            "中国话": "中文",
            "普通话": "中文",
            "汉语": "中文",
            "日文": "日语",
            "英文": "英语",
            "中国": "中文",
            "韩文": "韩语",
        }
        languages = dic["languages"]
        new_languages = []
        for language in languages:
            if language in ["不详", "暂无", "其他", "其它"]:
                continue
            elif language in languages_mapping:
                new_languages.append(languages_mapping[language])
            else:
                new_languages.append(language)
        dic["languages"] = list(set(new_languages))
        return dic

    @staticmethod
    def correct_islast(preprocess_context):
        """
        修正islast字段
        :param preprocess_context:
        :return:
        """
        if preprocess_context["channel"] not in ["综艺", ]:
            preprocess_context["ext"]["islast"] = True

        return preprocess_context

    @staticmethod
    def correct_episodes(preprocess_context):
        episode_str = preprocess_context["episode"]
        try:
            int(episode_str)
        except ValueError:
            pass
        else:
            preprocess_context["episode"] = str(int(episode_str))
        return preprocess_context

    @staticmethod
    def correct_score(preprocess_context):
        score = preprocess_context["score"]
        doubanscore = preprocess_context.get("doubanscore", 0)
        if score in [0, "nan", None, "0", ""]:
            if doubanscore not in [0, "nan", None, "0", ""]:
                score = doubanscore
            else:
                score = 0
        preprocess_context["score"] = float(score)

        return preprocess_context

    @staticmethod
    def correct_reference_time(preprocess_context):
        """修正reference_time字段"""
        if preprocess_context["publishyear"] in [0, "", None] and preprocess_context["channel"] == "综艺":
            # 对于发行年份是0的数据，先采用last字段做参考，没有的话，再用updatetime做参考
            if len(preprocess_context["last"]) >= 8 and preprocess_context["last"] != "00000000":
                preprocess_context["reference_time"] = int(preprocess_context["last"][0:4])

            else:
                try:
                    updatetime = int(preprocess_context["updatetime"].split("-")[0])
                except ValueError:
                    updatetime = 0
                preprocess_context["reference_time"] = updatetime

        else:
            preprocess_context["reference_time"] = preprocess_context["publishyear"]
        return preprocess_context

    @staticmethod
    def correct_regions(preprocess_context):
        """修正地区字段"""
        region_mapping_dic = {
            "中华人民共和国": "大陆",
            "中国": "大陆",
            "中国大陆": "大陆",
            "国产": "大陆",
            "国内": "大陆",
            "内地": "大陆",
        }
        regions = preprocess_context["regions"]
        new_region_list = []
        for region in regions:
            if region in region_mapping_dic:
                new_region = region_mapping_dic[region]
                new_region_list.append(new_region)

            elif region in ["欧美", "美欧"]:
                new_region_list.extend(["美国", "英国"])

            elif region in ["日韩", "韩日"]:
                new_region_list.extend(["日本", "韩国"])

            elif region in ["港台", "台港"]:
                new_region_list.extend(["香港", "台湾"])

            elif region in ["港澳", "澳港"]:
                new_region_list.extend(["香港", "澳门"])

            elif region in ["None", "", "不详", "暂无", "其他", "其它"]:
                continue

            else:
                new_region_list.append(region)
        preprocess_context["regions"] = list(set(new_region_list))
        return preprocess_context

    @staticmethod
    def correct_aliases(preprocess_context):
        """修正别名字段"""
        pattern = re.compile("^(\D+[0-9])[：:]")
        if preprocess_context["vendor"] in ["13", "15"] and preprocess_context["status"] == 1 and preprocess_context[
            "type"] == 1 and \
                preprocess_context["channel"] in ["电影"] and preprocess_context["ext"]["islast"] is True:

            find_pattern = re.findall(pattern, preprocess_context["title"])
            if find_pattern:
                if find_pattern[0] in preprocess_context["aliases"]:
                    pass
                else:
                    preprocess_context["aliases"].append(find_pattern[0])
        return preprocess_context

    # TODO 待验证
    @staticmethod
    def correct_iqiyi_type(line_json_dic):  # iqiyi存在单视频被包装成专辑，此处进行纠正
        if line_json_dic["vendor"] == "11" and line_json_dic["channel"] != "电影" and line_json_dic["aid"][-2:] in ["00",
                                                                                                                  "07"]:
            line_json_dic["type"] = 5
        return line_json_dic

    @staticmethod
    def add_video_name(preprocess_context):
        """
        添加video_name字段
        :param preprocess_context:
        :return:
        """
        params = preprocess_context["params"]
        pattern = re.compile("video_name=(.*)")

        if "video_name" in params:
            params_list = params.split("|")
            for param in params_list:
                if "video_name" in param:
                    video_name = re.findall(pattern, param)
                    preprocess_context["video_name"] = video_name[0]
                else:
                    continue
        else:
            preprocess_context["video_name"] = preprocess_context["title"]
        return preprocess_context

    @staticmethod
    def correct_publishdate(preprocess_context):
        if "publishdate" not in preprocess_context:
            preprocess_context["publishdate"] = ""
        if preprocess_context["channel"] == "综艺":
            if preprocess_context["publishdate"] == "":
                if preprocess_context["publishyear"] != 0:  # 当综艺的date是0时，先参考publishyear
                    preprocess_context["publishdate"] = str(preprocess_context["publishyear"]) + "0630"
                else:
                    if len(preprocess_context["last"]) >= 8 and preprocess_context["last"] != "00000000":
                        # publishyear没有的话，参考last

                        preprocess_context["publishdate"] = preprocess_context["last"][0:8]
                    else:  # last再没有的话，参考updatetime
                        updatetime = preprocess_context["updatetime"]
                        try:
                            updatetime_new = updatetime.replace("-", "")[0:8]
                        except IndexError:
                            updatetime_new = "0"
                        preprocess_context["publishdate"] = updatetime_new
        else:
            if preprocess_context["publishdate"] != "":
                if preprocess_context["publishyear"] == 0:  # 所有非综艺，原始出场年份为0,publishdate全部初始化为0
                    preprocess_context["publishdate"] = "0"
            else:
                if preprocess_context["publishyear"] == 0:
                    preprocess_context["publishdate"] = "0"
                else:
                    preprocess_context["publishdate"] = str(preprocess_context["publishyear"]) + "0630"
        if preprocess_context["publishdate"] in ["", None]:
            preprocess_context["publishdate"] = 0
        else:
            try:
                preprocess_context["publishdate"] = int(
                    preprocess_context["publishdate"].replace("-", "").replace("/", ""))
            except ValueError:
                preprocess_context["publishdate"] = 0
        return preprocess_context

    @staticmethod
    def correct_channel(line_json_dic):  # 临时方案:修改部分aid为早教,用于解决动画片的query时部分数据不满足要求的问题
        disrecall_aid_list = ["mzc00200yfqn794",
                              "mzc00200gbob9pw",
                              "mzc00200y4gqzq9",
                              "mzc002000h2z9w6",
                              "mzc00200t4bkwp5",
                              "mzc00200k66s1kn",
                              "mzc00200eemmflf",
                              "mzc00200i8wyf8h",
                              "mzc00200tjbfwp0",
                              "mzc00200kv8fhvo",
                              "mzc00200dftxs0w",
                              "mzc00200gc80prh",
                              "mzc00200hytck43",
                              "mzc002004a4ihtv",
                              "mzc002000lg1d1y",
                              "mzc002002gw6bsl",
                              "mzc002000omapgd",
                              "mzc00200auwkvij",
                              "mzc00200qm2abvj",
                              "mzc002006osm7a1",
                              "mzc00200tkjwox1",
                              "mzc00200ssme58x",
                              "mzc00200t4bkwp5",
                              "mzc00200k66s1kn",
                              "mzc0020073z9y08",
                              "mzc00200lp3pkr6",
                              "mzc002000i46ppr",
                              "mzc00200l9vdsja",
                              "mzc00200dtfdodm",
                              "mzc002002sk6ng9",
                              "mzc00200qm8knaz",
                              "mzc00200xkdmbjk"]
        if line_json_dic["aid"] in disrecall_aid_list:
            line_json_dic["channel"] = "早教"
            line_json_dic["channelid"] = "999"
        return line_json_dic

    def if_break_sign_in_field(self, preprocess_context):  # 去除数据中的换行符,左右两边的空格
        for field in preprocess_context:
            field_value = preprocess_context[field]
            try:
                field_type = self.album_filed_list_type_mapping_finally[field]
            except KeyError:
                pass
            else:
                if field_type == "string":
                    field_value = field_value.replace("\r", "")
                    field_value = field_value.replace('\n', '')
                    field_value = field_value.replace('\t', '')
                    field_value = field_value.strip()
                    preprocess_context[field] = field_value
                elif field_type == "list":
                    new_field_list = []
                    for index, one_field_value in enumerate(field_value):
                        if type(one_field_value) == str:
                            one_field_value = one_field_value.replace("\r", "")
                            one_field_value = one_field_value.replace("\n", "")
                            one_field_value = one_field_value.replace("\t", "")
                            one_field_value = one_field_value.strip()
                            new_field_list.append(one_field_value)
                        elif type(one_field_value) == dict:
                            one_field_value_list = one_field_value[list(one_field_value.keys())[0]]
                            new_one_field_value_list = []
                            for one_field_value_string in one_field_value_list:
                                one_field_value_string = one_field_value_string.replace("\r", "")
                                one_field_value_string = one_field_value_string.replace("\n", "")
                                one_field_value_string = one_field_value_string.replace("\t", "")
                                one_field_value_string = one_field_value_string.strip()
                                new_one_field_value_list.append(one_field_value_string)
                            one_field_value[list(one_field_value.keys())[0]] = new_one_field_value_list
                            new_field_list.append(one_field_value)
                    preprocess_context[field] = new_field_list
                elif field_type == "map":
                    new_map_dic = {}
                    for sub_field_key in preprocess_context[field]:
                        sub_field_value = preprocess_context[field][sub_field_key]
                        if type(sub_field_value) == str:
                            sub_field_value = sub_field_value.replace("\r", "")
                            sub_field_value = sub_field_value.replace("\n", "")
                            sub_field_value = sub_field_value.replace("\t", "")
                            sub_field_value = sub_field_value.strip()
                            new_map_dic[sub_field_key] = sub_field_value
                        elif type(sub_field_value) == list:
                            new_sub_field_value_list = []
                            for one_value in sub_field_value:
                                one_value = one_value.replace("\r", "")
                                one_value = one_value.replace("\n", "")
                                one_value = one_value.replace("\t", "")
                                one_value = one_value.strip()
                                new_sub_field_value_list.append(one_value)
                            new_map_dic[sub_field_key] = new_sub_field_value_list
                        else:
                            new_map_dic[sub_field_key] = preprocess_context[field][sub_field_key]
                    preprocess_context[field] = new_map_dic
        return preprocess_context

    def split_field(self, preprocess_context):
        SS = SplitStr()
        for field in self.need_split_field_list:
            if field not in preprocess_context:  # 有些数据可能没被补充,增添字段设置为空
                field_type = self.album_filed_list_type_mapping_finally[field]
                if field_type == "list":
                    preprocess_context[field] = []
                elif field_type == "map":
                    preprocess_context[field] = {}
            field_value = preprocess_context[field]
            field_type = self.album_filed_list_type_mapping_finally[field]
            if field_type == "list":
                new_field_value = []
                for index, one_field_value in enumerate(field_value):
                    if type(one_field_value) == str:
                        one_split_field_list = SS.split_str(one_field_value)
                        new_field_value = new_field_value + one_split_field_list
                        new_field_value = list(set(new_field_value))
                    elif type(one_field_value) == dict:  # tag的统计字段
                        new_source_tag_list = []
                        source_tag_list = one_field_value[list(one_field_value.keys())[0]]
                        for one_source_tag_string in source_tag_list:
                            one_split_source_tag_list = SS.split_str(one_source_tag_string)
                            new_source_tag_list = new_source_tag_list + one_split_source_tag_list
                        new_source_tag_list = list(set(new_source_tag_list))
                        one_field_value[list(one_field_value.keys())[0]] = new_source_tag_list
                        new_field_value.append(one_field_value)
                preprocess_context[field] = new_field_value
            elif field_type == "string":
                new_field_value = SS.split_str(field_value)
                new_field_value = list(set(new_field_value))
                preprocess_context[field] = new_field_value
        return preprocess_context

    def if_fieldvalue_invaild(self, preprocess_context):
        """
        消除标点符号
        :param preprocess_context:
        :return:
        """
        SC = StrClean()
        for field in self.clean_invalid_field_list:
            if field == "producers":
                new_producer_list = []
                for producer in preprocess_context[field]:
                    producer = SC.clean_sign(producer)
                    producer = SC.if_str_is_binary_code(producer)
                    producer = SC.if_invaild_words(producer)
                    producer = SC.if_invaild_number(producer)
                    new_producer_list.append(producer)
                new_producer_list = SC.if_null_in_list(new_producer_list)
                preprocess_context[field] = new_producer_list
            elif field == "statistic_item":
                for source_tags_dic in preprocess_context[field]:
                    source_tags_list = source_tags_dic[list(source_tags_dic.keys())[0]]
                    new_source_tags_list = []
                    for tag in source_tags_list:
                        tag = SC.if_brackets_in_str(tag)
                        tag = SC.if_str_is_binary_code(tag)
                        tag = SC.if_invaild_words(tag)
                        tag = SC.if_only_need_chinese(tag)
                        tag = SC.if_invaild_one_lence(tag)
                        new_source_tags_list.append(tag)
                    new_source_tags_list = SC.if_null_in_list(new_source_tags_list)
                    source_tags_dic[list(source_tags_dic.keys())[0]] = new_source_tags_list
            elif field == "tags":
                new_tag_list = []
                for tag in preprocess_context[field]:
                    tag = SC.if_brackets_in_str(tag)
                    tag = SC.if_str_is_binary_code(tag)
                    tag = SC.if_invaild_words(tag)
                    tag = SC.if_only_need_chinese(tag)
                    tag = SC.if_invaild_one_lence(tag)
                    new_tag_list.append(tag)
                new_tag_list = SC.if_null_in_list(new_tag_list)
                preprocess_context[field] = new_tag_list
            elif field == "title":
                title = preprocess_context["title"]
                title = title.replace('"', "")
                title = title.replace('“', "")
                title = title.replace('”', "")
                title = title.replace("'", "")
                title = title.replace("\\", "")
                title = title.replace("《", "")
                title = title.replace("》", "")
                title = title.replace("!", "")
                title = title.replace("！", "")
                title = title.replace(",", "")
                title = title.replace("，", "")
                title = title.replace("?", "")
                title = title.replace("。", "")
                title = title.replace("(", "")
                title = title.replace(")", "")
                title = title.replace("（", "")
                title = title.replace("）", "")
                preprocess_context["title"] = title
            else:
                new_field_value = []
                for fild_value in preprocess_context[field]:
                    fild_value = SC.clean_sign(fild_value)
                    fild_value = SC.if_str_is_binary_code(fild_value)
                    fild_value = SC.if_invaild_words(fild_value)
                    new_field_value.append(fild_value)
                new_field_value = SC.if_null_in_list(new_field_value)
                preprocess_context[field] = new_field_value
        return preprocess_context

    def repetitive_items(self, preprocess_context):
        preprocess_context["repetitive_items_prior_eliminated"] = []  # 牌照屏蔽
        return preprocess_context

    def process_media(self, media_dict):
        # TODO 以下逻辑参考 搜索三期清洗脚本重构,待整理优化
        preprocess_context = copy(media_dict)

        # check_data 函数填充空值、字段转小写
        preprocess_context = self.check_data(preprocess_context)

        # 转换格式，将原来由分隔符分割的
        preprocess_context = self.params_field_for_combine(preprocess_context, ["actors", "directors"])

        # 更新/生成is_last字段
        preprocess_context = self.correct_islast(preprocess_context)

        # 更新reference_time
        preprocess_context = self.correct_reference_time(preprocess_context)

        # 更新regions
        preprocess_context = self.correct_regions(preprocess_context)
        preprocess_context = self.correct_actors(preprocess_context)
        preprocess_context = self.correct_entitle(preprocess_context)
        preprocess_context = self.correct_languages(preprocess_context)
        preprocess_context = self.correct_score(preprocess_context)

        preprocess_context = self.correct_aliases(preprocess_context)
        preprocess_context = self.add_video_name(preprocess_context)
        preprocess_context = self.correct_publishdate(preprocess_context)
        preprocess_context = self.correct_iqiyi_type(preprocess_context)
        preprocess_context = self.correct_channel(preprocess_context)

        # 旧版媒资融合、爬虫标签
        # preprocess_context = self.combine_with_old_media(preprocess_context)

        preprocess_context = self.if_break_sign_in_field(preprocess_context)  # 去除换行符
        preprocess_context = self.split_field(preprocess_context)  # 字段拆分
        preprocess_context = self.if_fieldvalue_invaild(preprocess_context)  # 不合法数据清洗

        # TODO 前面生成的id被删除了
        id_only = preprocess_context["vendor"] + preprocess_context["aid"]
        preprocess_context.update({"id": id_only})
        preprocess_context.pop("statistic_item")
        preprocess_context = self.repetitive_items(preprocess_context)  # 屏蔽牌照
        preprocessed_media_data = copy(preprocess_context)
        del preprocess_context
        return preprocessed_media_data


class TagsCleanNode(BaseNode):
    """通用标签字段清洗节点"""

    def __init__(self, name, **kwargs):
        BaseNode.__init__(self, name=name, **kwargs)
        # 收集以下字段信息，用于筛选tags
        self.collect_field_list = ["producers", "actors", "directors", "languages", "regions", "awards", "characters",
                                   "writer"]
        self.mongo_conn = MongoHelper()
        # 加载历史字段值
        self.collect_field_value = defaultdict(lambda: set())
        self.load_collection_field_value()

        # 加载基础参考词
        self.benchmark_data_name_ch = set(self.load_benchmark_data(coll="ref_name_ch"))  # 加载中文名
        self.benchmark_data_name_en = set(self.load_benchmark_data(coll="ref_name_en"))  # 加载英文名
        self.benchmark_data_meaningless_word = set(self.load_benchmark_data(
            coll="ref_no_meaning_word"))  # 加载无意义词

    def load_benchmark_data(self, coll):
        """用于参考的常用词"""
        # {data_id:"", data_value:""}
        benchmark_data_list = [i["data_value"] for i in self.mongo_conn.find(coll=coll, dict_filter={})]
        return list(set(benchmark_data_list))

    def save_field_dataset(self, field_name, field_value: str):
        """写入字段值"""
        data_id = hashlib.md5(field_value.encode()).hexdigest()
        insert_data = {"data_id": data_id, "data_value": field_value}
        self.mongo_conn.replace_one(coll=f"ref_{field_name}", dict_filter={"data_id": data_id},
                                    dict_replacement=insert_data, upsert=True)

    def load_collection_field_value(self):
        """读取历史字段值"""
        for field_name in self.collect_field_list:
            field_history_value_list = [i["data_value"] for i in
                                        self.mongo_conn.find(coll=f"ref_{field_name}", dict_filter={})]
            self.collect_field_value[field_name] = set(field_history_value_list)

    def save_collection_field_value(self, media):
        """获取当前媒资字段值并保存"""
        for field_name in self.collect_field_list:
            media_field_value = media.get(field_name, None)

            if media_field_value:
                for value in media_field_value:
                    if value not in self.collect_field_value[field_name]:
                        self.collect_field_value[field_name].add(value)  # TODO 理论上需要加锁
                        self.save_field_dataset(field_name=field_name, field_value=value)

    def if_tag_unsuitable(self, tag, title, aliases):
        """通过别名和标题, tag清洗"""
        if tag:
            it1 = iter(["电影", "电视剧", "综艺", "动漫", "动画", "的"])
            str1 = ""
            for key in it1:
                str1 = tag.replace(key, "")
            if str1[-1] == "剧" and len(str1) >= 3:
                str1 = str1.replace("剧", "")
            if len(str1) >= 5:
                return ""
            if str1[0] in self.benchmark_data_name_ch:
                return ""
            if str1[0] in self.benchmark_data_name_en and len(str1) in [3, 4]:
                return ""
            if str1 in self.benchmark_data_meaningless_word:
                return ""
            if str1 == title or str1 in aliases:
                return ""
            if str1 in ["不详", "暂无", "其他", "其它"]:
                return ""
            else:
                return str1
        else:
            return ""

    def if_tag_in_other_field(self, tag):
        """判断tag值是否出现在其它字段中"""
        for field_name, file_value_list in self.collect_field_value.items():
            if tag in file_value_list:
                return True
        else:
            return False

    def add_regions_tags(self, item):
        regions_list = item["regions"]
        region_tags_mapping = {
            "美国": "美剧",
            "日本": "日剧",
            "中国": "国产剧",
            "大陆": "国产剧",
            "内地": "国产剧",
            "英国": "英剧",
            "泰国": "泰剧",
            "法国": "法剧",
            "韩国": "韩剧",
            "印度": "印剧",
            "俄罗斯": "俄剧",
            "德国": "德剧"
        }
        if item["channel"] == "电视剧":
            if regions_list:
                tags = item["tags"]

                for region in regions_list:
                    tag = region_tags_mapping.get(region, "")
                    if tag:
                        tags.append(tag)

                item["tags"] = list(set(tags))
        return item

    @staticmethod
    def add_subtitle_tags(item):
        """从标题补充一些标签"""
        tags = item["tags"]
        title = item.get("title", "")
        if "剧场" in title:
            tags.extend(["剧场版", "剧场"])
        if "真人" in title:
            tags.extend(["真人版", "真人"])

        item["tags"] = list(set(tags))
        return item

    def process_media(self, media_dict: dict):
        # 收集字段信息
        self.save_collection_field_value(media=media_dict)

        # 过滤无用tag
        new_media_tags = []
        for tag in media_dict["tags"]:
            new_tag = self.if_tag_unsuitable(tag, media_dict["title"], media_dict["aliases"])

            if new_tag and not self.if_tag_in_other_field(new_tag):  # todo 存在可以做标签的人名、地名
                new_media_tags.append(new_tag)

        media_dict["tags"] = new_media_tags

        # 添加地区标签
        item = self.add_regions_tags(media_dict)

        # 添加剧场版、真人版
        item = self.add_subtitle_tags(item)
        return item


class TitleCleanNode(BaseNode):
    """通用title字段清洗节点"""

    def __init__(self, name, **kwargs):
        self.mongo_conn = MongoHelper()
        BaseNode.__init__(self, name=name, **kwargs)

    def process_media(self, media_dict):
        data_id = media_dict["id"]
        channel = media_dict["channel"]
        title = media_dict["title"]
        season = media_dict.get("season", "")

        pattern1 = re.compile("(第([一二三四五六七八九十0-9]+)([集季部期]))")
        extract_season_text = [title, ]
        extract_season_text.extend(media_dict.get("aliases", []))  # 尝试从别名中提取season
        for text in extract_season_text:
            find_pattern = re.findall(pattern1, text)
            if find_pattern:
                if find_pattern[0][2] in ["期", "集"]:  # 如果片名里面有集/期作为专辑
                    title = title.replace(find_pattern[0][0], "")  # 去掉片名中的期数据,季的信息后面会纠正
                    media_dict["title"] = title
                else:
                    season = str(chinese2num(find_pattern[0][1]))
                    media_dict["title"] = media_dict["title"].replace(find_pattern[0][0],
                                                                      "").strip()  # 去掉片名中季的数据
                    break

        if not season and channel in ["电影", "电视剧", "动漫", "少儿"]:
            # 尝试从分集数据标题中提取
            for video in self.mongo_conn.find(coll=settings.PUB_VIDEO_COLL, dict_filter={"albumid": data_id}):
                season = extract_season_from_title(video["video_title"])
                if season:
                    break
        media_dict["season"] = season
        media_dict["title"] = media_dict["title"].strip()  # 去除两端空格
        return media_dict


class MergeTaggingNode(BaseNode):
    """合并标注数据节点"""

    def __init__(self, name, **kwargs):
        BaseNode.__init__(self, name=name, **kwargs)
        self.mongo_conn = MongoHelper()

    @staticmethod
    def merge_tagging_data(raw_media, tagging_media):
        """
        通过标注数据修正媒资字段
            {"id": "13swglm5xrkvvz91o",
            "vendor": "13",
            "aid": "swglm5xrkvvz91o",

            # reference 级别数组字段会被融合到原媒资，其它字段只有原值为空时才会使用
            "reference": {"title": "美女与野兽",
                          "video_name": "美女与野兽 第一季",
                          "entitle": "",
                          "brief": "",
                          "publishdate": 20140611,
                          "reference_time": 2014,
                          "publishyear": 2014,
                          "score": 7,
                          "season": "",
                          "fullpinyin": [],
                          "simplepinyin": [],
                          "aliases": [],
                          "regions": ["欧美", "美国", "英国"],
                          "languages": ["中文"],
                          "tags": ["亲情", "兴奋", "农村", "剧情", "动作", "动画", "华丽", "喜剧", "国内", "国语", "夫妻关系", "奇幻", "家庭", "幽默", "心理",
                                   "荒诞", "虚拟时空", "道德", "魔幻"],
                          "actors": ["文森特·卡索", "蕾雅·赛杜"],
                          "directors": ["克里斯多夫·甘斯"],
                          "producers": [],
                          "characters": ["Beast", "Belle", "Gaston", "Le marchand", "Lumiere", "商人", "贝儿公主"],
                          "awards": [],
                          "writer": ["Brian Pimental", "Bruce Woodside", "Kevin Harkey", "Robert Lence", "Tom Ellery", "乔兰福特",
                                     "伯尼马丁森", "克里斯多夫甘斯", "克里斯桑德斯", "凯利阿斯博瑞", "布兰达查普曼", "桑德拉沃安", "琳达伍尔芙顿", "罗杰阿勒斯", "让谷克多"],
                          },

            # correct 用于纠错，数组字段删除指定元素，其它类型字段直接覆盖原值
            "correct": {
                "tags_blacklist": [],
                "publishyear_fix": 2010
            }
            }
        """
        reference_data = tagging_media["reference"]  # 参考结果,数组字段会被融合到原媒资，其它字段只有原值为空时才会使用
        correct_data = tagging_media["correct"]  # 强制修正结果,数组字段中需要移除的元素和需要被覆盖的

        need_merge_field = ["publishyear", "score", "regions", "actors", "tags", "directors", "characters", "awards",
                            "languages", "writer", "season", "publishdate", "reference_time"]
        for field in need_merge_field:

            if reference_data.get(field, "") and isinstance(reference_data[field], list):
                # 对于数组类字段，直接合并 0416
                merged_field_value = raw_media[field] + reference_data.get(field, [])
                raw_media[field] = list(set(merged_field_value))

            else:
                # 非数组字段，原值为空时，使用标注数据填充
                if not raw_media[field]:
                    if reference_data.get(field, ""):
                        raw_media[field] = reference_data[field]

        # 强制修正字段值
        for correct_filed_name, correct_filed_value in correct_data.items():
            if correct_filed_name.endswith("_blacklist"):
                raw_filed_name = correct_filed_name.replace("_blacklist", "")
                raw_media[raw_filed_name] = list(set(raw_media[raw_filed_name]) - set(correct_filed_value))
            elif correct_filed_name.endswith("_fix"):
                raw_filed_name = correct_filed_name.replace("_fix", "")
                raw_media[raw_filed_name] = correct_filed_value
            else:
                logging.error(f"标注数据异常{tagging_media}")
        return raw_media

    def process_media(self, media_dict):
        """
        合并标注数据, 主要为了保证三期数据中的字段不丢失！！！！
        """
        # 1. 根据aid合并标注数据    todo 小米牌照aid和其它牌照重复，但内容没有相关性
        match_by_aid = self.mongo_conn.find_one(coll=settings.REFER_TAGGING,
                                                dict_filter={"id": media_dict["vendor"] + media_dict["aid"]})
        if match_by_aid:
            media = self.merge_tagging_data(media_dict, match_by_aid)

            # # 2. 根据title等信息合并标注
            # # todo 可能存在同名不同影片
            return media
        else:
            return media_dict


class AlbumUpdateRelativeNode(BaseNode):
    """更新相关联数据节点"""

    def __init__(self, name, **kwargs):
        BaseNode.__init__(self, name=name, **kwargs)
        self.mongo_conn = MongoHelper()
        self.processed_album_coll = settings.PUB_ALBUM_COLL
        self.processed_video_coll = settings.PUB_VIDEO_COLL

    def process_media(self, media_dict):
        """更新关联的数据"""
        # 更新到mongodb
        data_id = media_dict["id"]
        media_dict.update({"data_state": settings.PENDING, "data_timestamp": int(time.time() * 1000)})
        # 数组字段重新排序
        for value in media_dict.values():
            if isinstance(value, list):
                value.sort()

        self.mongo_conn.replace_one(coll=self.processed_album_coll, dict_filter={"id": data_id},
                                    dict_replacement=media_dict)

        # 更新关联的分集数据, 分集数据使用了专辑的标题等字段，需要重新导入    TODO 只有关联字段更新时，才需要重新导入分集数据
        self.mongo_conn.update(coll=self.processed_video_coll, dict_filter={"albumid": data_id},
                               dict_update={"$set": {"data_state": settings.PENDING}})

        return media_dict


class TencentSimilarMediaFilterNode(BaseNode):
    """腾讯去同质化媒资需求
    1.  vendor：除了 腾讯南传、腾讯未来、雷咚咚、大健康 以外的所有牌照方
        channel： "少儿", "芒果-动漫","爱奇艺-母婴"
        type: 1
        以上范围的媒资如果不在白名单则不允许上架，固定为status=0
    2. 所有媒资通过雷鸟接口更新repetitive_items_prior_eliminated 字段，由云端判断是否展示
    """

    def __init__(self, name):
        self.vendorWhitelist = {"13", "15", "310", "57"}  # 腾讯、雷咚咚、大健康不需要下架
        self.aidWhitelist = set()
        self.channelBlacklist = {"少儿", }  # 需要屏蔽的频道
        whitelistFile = os.path.join(settings.BASE_DIR, "pipeline/nodes_vendor/whitelist.txt")
        with open(whitelistFile, encoding="utf-8") as f:
            for media_id in f.read().splitlines():
                self.aidWhitelist.add(media_id)

        BaseNode.__init__(self, name)

    def process_media(self, item: dict):
        """下架同质化媒资"""
        if item["type"] != 1:  # 只下架正片！！！
            return item

        if item["vendor"] in ["12", ]:  # 芒果牌照需要下架动漫频道
            self.channelBlacklist.add("动漫")

        if item["vendor"] in ["11", ]:  # 爱奇艺牌照需要下架动漫频道
            self.channelBlacklist.add("母婴")

        if item["vendor"] not in self.vendorWhitelist and item["channel"] in self.channelBlacklist:
            if item["id"] not in self.aidWhitelist:
                item["status"] = 0
        return item


class TaintedCelebrityFilterNode(BaseNode):
    """广电总局需求,下架劣迹艺人媒资"""

    def __init__(self, name):
        self.aidBlacklist = set()
        blacklistFile = os.path.join(settings.BASE_DIR, "pipeline/nodes_vendor/actor_blacklist.txt")
        with open(blacklistFile, encoding="utf-8") as f:
            for media_id in f.read().splitlines():
                self.aidBlacklist.add(media_id)
        BaseNode.__init__(self, name)

    def process_media(self, item: dict):
        """下架媒资"""
        if item["id"] in self.aidBlacklist:
            item["status"] = 0
        return item


class LeiniaoBlackListNode(BaseNode):
    """雷鸟需求,下架部分不在少儿频道下（母婴，教育）的少儿媒资"""

    def __init__(self, name):
        self.aidBlacklist = set()
        blacklistFile = os.path.join(settings.BASE_DIR, "pipeline/nodes_vendor/leiniao_blacklist.txt")
        with open(blacklistFile, encoding="utf-8") as f:
            for media_id in f.read().splitlines():
                self.aidBlacklist.add(media_id)
        BaseNode.__init__(self, name)

    def process_media(self, item: dict):
        """下架媒资"""
        if item["id"] in self.aidBlacklist:
            item["status"] = 0
        return item


class ClearImaxMediaNode(BaseNode):
    """下架imax媒资"""

    def __init__(self, name):
        self.imax_aid_list = {"t6l0dp9r6zl69gh", "mzc0020013nz49g", "mzc00200ulsjvgm", "mzc00200eng936w",
                              "mzc00200hahcwsr", "mzc00200533jgtw", "mzc002000purhoe", "mzc00200rrjpa4g",
                              "mzc00200v652ylv", "mzc00200l25s02n", "mzc00200ssksiiq", "mzc00200ysk6ofa",
                              "mzc00200xnl9pfd", "mzc00200g6om2s0", "mzc00200hfpphlh", "mzc00200xpd3x7n",
                              "mzc00200ycucwye", "mzc00200opynujj", "mzc00200edcv0nj", "mzc00200p8pa1bm",
                              "mzc00200qg600ku", "mzc00200tj3vxkp", "mzc0020055z1daf", "mzc002007w147wa",
                              "mzc00200w109u23", "mzc00200saqwuf0", "mzc00200behujyw", "mzc00100ty5ghjs",
                              "mzc00200wqu0b8h", "mzc00200b17v40s"}
        BaseNode.__init__(self, name)

    def process_media(self, item: dict):
        """下线imax媒资"""
        if item["aid"] in self.imax_aid_list:
            item["status"] = 0
        return item


class ClearErrorMediaNode(BaseNode):
    """下架无法播放或者异常媒资"""

    def __init__(self, name):
        self.aid_blacklist = {"daac4bc22f0a4bb49971", "3466ac702f7d11e5b522", "cc059de6962411de83b1", "327129",
                              "326126", "cbffccea962411de83b1", "cc059de6962411de83b1", "bcff468ca516461fb078",
                              "36150", "327129", "326126", "f2fd47a461ad11e0bea1", "a5fd33549e6611e2b356", "cc0f4558962411de83b1", "cbfa220e962411de83b1"}
        BaseNode.__init__(self, name)

    def process_media(self, item: dict):
        """下线异常媒资"""
        if item["aid"] in self.aid_blacklist:
            item["status"] = 0
        return item


class SpecialCaseNode(BaseNode):
    """特殊处理的媒资"""

    def process_media(self, item: dict):
        """处理媒资"""
        if item["aid"] in {"3hcnhz6ys9njhun", "2biwpzdbc21mk04", "2tk2e2v12j4jcm5", "lvrj4x86a4xyv4o", "1weku8f23n0j12s"}:
            item["type"] = 1  # 实际是正片
        if item["aid"] in {"k4evtr5q0uktpwy", } and item["type"] == 1:
            item["type"] = 2  # 实际为片花

        for tag in ['校园暴力', '校园欺凌', '校园霸凌']:  # 补充同义标签
            if tag in item["tags"]:
                item["tags"].extend(['校园暴力', '校园欺凌', '校园霸凌'])
                item["tags"] = list(set(item["tags"]))
                break

        if item["aid"] in {"acc05470a06f11df97c0", "31360308", "brm0izb40uvr698", "kc055hlduv68bxm"}:   # 熊出没注意、有熊出没，影响熊出没体验
            item["status"] = 0
        return item
