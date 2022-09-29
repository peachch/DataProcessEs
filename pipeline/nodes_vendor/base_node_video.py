
import logging
import re
import time
from copy import copy

from consecution import Node

import settings
from common.mongo_helper import MongoHelper
from utils.data_utils import chinese2num


def extract_season_from_title(video_title):
    # 基础结构： Name+分隔符/空格+Season

    # Name+[空格]？+第X季：权力的游戏 第一季
    # Name+[空格]+第X期
    # Name+（第X季）: 中国好歌曲（第三季）
    # Name+[-]+第X季：银魂-第一季
    season = ""
    # todo 期和集的数据
    pattern1 = re.compile("(第([一二三四五六七八九十0-9]{1,})([季部期]))")
    find_pattern = re.findall(pattern1, video_title)
    if find_pattern:
        if find_pattern[0][2] in ["季", "部"]:  # 如果片名里面有季/部作为专辑
            season = str(chinese2num(find_pattern[0][1]))
    return season


class BaseNode(Node):
    """打印日志、基础函数重写"""

    def process(self, item):
        processed_item = self.process_media(item)
        logging.debug(
            f"node {self.name}, data_id {processed_item.get('id', processed_item['vendor'] + processed_item['vid'])},  output {processed_item}")
        self.push(processed_item)

    def process_media(self, media_dict):
        raise NotImplementedError


class VideoPreprocessNode(BaseNode):
    """分集预处理节点"""
    def __init__(self, name):
        BaseNode.__init__(self, name=name)
        self.field_type_mapping = {
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

        # 需要预处理的字段
        self.limited_field_list = ["status", "type", "vpic", "title", "vid", "quality", "playurl", "needpay", "index",
                                   "episode",
                                   "duration", "updatetime", "vendor", "channel", "albumids"]

        self.mongo_conn = MongoHelper()

    def process_media(self, media_dict):
        preprocess_context = self.filed_lower(media_dict)  # 把所有字段转小写
        preprocess_context = self.if_filed_missing_or_invaild(preprocess_context)
        preprocess_context = self.title_lower(preprocess_context)
        preprocess_context = self.correct_episodes(preprocess_context)
        preprocess_context = self.get_extent_title_data(preprocess_context)
        preprocess_context = self.rename_fields(preprocess_context)
        return preprocess_context

    def rename_fields(self, video_line_dic):
        for field in list(video_line_dic.keys()):
            if field in self.limited_field_list:
                if field in ["status", "type", "vpic", "title"]:
                    video_line_dic["video_" + field] = video_line_dic[field]
                    video_line_dic.pop(field)

        return video_line_dic

    def correct_episodes(self, line_json_dic):
        episode_str = line_json_dic["episode"]
        try:
            int(episode_str)
        except ValueError:
            pass
        else:
            line_json_dic["episode"] = str(int(episode_str))
        return line_json_dic

    @staticmethod
    def get_extent_title_data(video_line_dic):
        """
        判断title是否包含集期的数据
        :return:填充index值的数据
        """
        pattern_a = re.compile("^(第([一二三四五六七八九十百千万0-9]{1,})([集期]))：")
        pattern_b = re.compile("^\[(第([一二三四五六七八九十百千万0-9]{1,})([集期]))\]：?")
        pattern_c = re.compile("(季([一二三四五六七八九十百千万0-9]{1,})([集期]))")
        pattern_d = re.compile("(第([一二三四五六七八九十百千万0-9]{1,})([集期]))")
        title = video_line_dic["title"]
        find_pattern_a = re.findall(pattern_a, title)
        find_pattern_b = re.findall(pattern_b, title)
        find_pattern_c = re.findall(pattern_c, title)
        # 针对电视剧或者其他channel
        find_pattern_d = re.findall(pattern_d, title)
        if find_pattern_a:  # 如果分集的片名中包含期的信息,填充给index
            if video_line_dic["index"] == "" or video_line_dic["index"] == 0 and video_line_dic["channel"] == "综艺" and \
                    video_line_dic["status"] == 1 and video_line_dic["type"] == 1 and video_line_dic["vendor"] in ["15",
                                                                                                                   "13"]:
                try:
                    video_line_dic["index"] = int(chinese2num(find_pattern_a[0][1]))
                except ValueError:
                    pass
                else:
                    video_line_dic["correct_index"] = True
        elif find_pattern_b:
            if video_line_dic["index"] == "" or video_line_dic["index"] == 0 and video_line_dic["channel"] == "综艺" and \
                    video_line_dic["status"] == 1 and video_line_dic["type"] == 1 and video_line_dic["vendor"] in ["15",
                                                                                                                   "13"]:
                try:
                    video_line_dic["index"] = int(chinese2num(find_pattern_b[0][1]))
                except ValueError:
                    pass
                else:
                    video_line_dic["correct_index"] = True
        elif find_pattern_c:
            if video_line_dic["index"] == "" or video_line_dic["index"] == 0 and video_line_dic["channel"] == "综艺" and \
                    video_line_dic["status"] == 1 and video_line_dic["type"] == 1 and video_line_dic["vendor"] in ["15",
                                                                                                                   "13"]:
                try:
                    video_line_dic["index"] = int(chinese2num(find_pattern_c[0][1]))
                except ValueError:
                    pass
                else:
                    video_line_dic["correct_index"] = True
        elif find_pattern_d:
            if video_line_dic["index"] == "" or video_line_dic["index"] == 0 and video_line_dic["channel"] in ["电视剧",
                                                                                                               "少儿",
                                                                                                               "动漫",
                                                                                                               "纪录片"] and \
                    video_line_dic["status"] == 1 and video_line_dic["type"] == 1 and video_line_dic["vendor"] in ["15",
                                                                                                                   "13"]:
                try:
                    video_line_dic["index"] = int(chinese2num(find_pattern_d[0][1]))
                except ValueError:
                    pass
        return video_line_dic

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
                    filed_type = self.field_type_mapping[field]
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


class VideoExtractSeasonNode(BaseNode):
    """从分集标题抽取season信息"""
    def __init__(self, name):
        BaseNode.__init__(self, name=name)
        self.processed_album_coll = settings.PUB_ALBUM_COLL
        self.mongo_conn = MongoHelper()

    def process_media(self, media_dict):
        aid_list = media_dict["albumids"]
        vendor = media_dict["vendor"]
        channel = media_dict["channel"]
        if channel in ["电影", "电视剧", "动漫", "少儿"]:
            season = extract_season_from_title(media_dict["video_title"])

            if season:
                # 更新关联的专辑数据
                for aid in aid_list:
                    album_id = vendor + aid
                    album = self.mongo_conn.find_one(coll=self.processed_album_coll,
                                                     dict_filter={"id": album_id, "season": ""})
                    if album:
                        # 更新后重新上传数据
                        self.mongo_conn.update(coll=self.processed_album_coll, dict_filter={"id": album_id},
                                               dict_update={"$set": {"data_state": settings.PENDING, "season": season}})
        return media_dict


class VideoExplodeNode(BaseNode):
    """根据aid拆分分集数据, 保存结果到mongodb"""

    def __init__(self, name):
        BaseNode.__init__(self, name=name)
        self.mongo_conn = MongoHelper()

    def process_media(self, media_dict):
        aid_list = media_dict["albumids"]
        vendor = media_dict["vendor"]
        vid = media_dict["vid"]

        for aid in aid_list:
            album_id = vendor + aid
            data_id = vendor + aid + vid
            video_exploded = copy(media_dict)
            video_exploded.pop("albumids")
            video_exploded["aid"] = aid
            video_exploded["albumid"] = album_id
            video_exploded["id"] = data_id

            # 时间戳最后的更新时间
            video_exploded.update({"data_state": settings.PENDING, "data_timestamp": int(time.time() * 1000)})
            self.mongo_conn.replace_one(coll=settings.PUB_VIDEO_COLL, dict_filter={"id": data_id},
                                        dict_replacement=video_exploded)
        return media_dict
