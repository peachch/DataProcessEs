import datetime
import logging
import re
import time
from concurrent.futures.thread import ThreadPoolExecutor
from functools import lru_cache

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.util import undefined
from retry import retry
from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer
from sklearn.metrics.pairwise import cosine_similarity
from tqdm import tqdm

import settings
from common.mongo_helper import MongoHelper
from utils.data_utils import chinese2num


class Confidence(object):  # 定义比较规则

    def rule1(self, actor, director, channel):  # 对电影电视剧
        rule1_aid = actor & director & channel  # actor/director/channel满足
        return rule1_aid

    def rule2(self, channel, moviename):  # 对动漫少儿综艺纪录片
        rule2_aid = channel & moviename
        return rule2_aid

    def compare_actors(self, actor_list, actor_dic):
        if not actor_list:
            match_aid_set = set()
        else:
            match_aid_set = set()
            for actor in actor_list:
                try:
                    find_set = actor_dic[actor]
                except KeyError:
                    continue
                else:
                    match_aid_set = match_aid_set.union(find_set)  # 将所有存在一个名字相同id并集
        return match_aid_set

    def compare_directors(self, director_list, director_dic):
        if not director_list:
            match_aid_set = set()
        else:
            match_aid_set = set()
            for director in director_list:
                try:
                    find_set = director_dic[director]
                except KeyError:
                    continue
                else:
                    match_aid_set = match_aid_set.union(find_set)  # 将所有存在一个名字相同id并集
        return match_aid_set

    def compare_channel(self, channel, channel_dic):
        try:
            match_channel_set = channel_dic[channel]
        except KeyError:
            match_channel_set = set()
        return match_channel_set

    def compare_year(self, publishyear, publishyear_dic):
        try:
            match_publishyear_set = publishyear_dic[publishyear]
        except KeyError:
            match_publishyear_set = set()
        return match_publishyear_set

    def compare_moviename(self, moviename, moviename_dic):
        try:
            match_moviename_set = moviename_dic[moviename]
        except KeyError:
            match_moviename_set = set()
        return match_moviename_set

    def compare_moviename_tfidf(self, dic1, dic2):
        moviename_1 = re.sub(u"([^\u4e00-\u9fa5\u0030-\u0039\u0041-\u005a\u0061-\u007a])", "",
                             dic1["video_name"])  # 去除所有符号只要中文英文和数字
        moviename_2 = re.sub(u"([^\u4e00-\u9fa5\u0030-\u0039\u0041-\u005a\u0061-\u007a])", "", dic2["video_name"])

        # 将中文数字转换为阿拉伯数字
        def repl_ch_num(match):
            ch_num = match.group()
            a_num = chinese2num(ch_num)
            return str(a_num)

        moviename_1 = re.sub(r"[零一二三四五六七八九十百千万]+", repl_ch_num, moviename_1)
        moviename_2 = re.sub(r"[零一二三四五六七八九十百千万]+", repl_ch_num, moviename_2)

        moviename_1_list = ' '.join(list(moviename_1))
        moviename_2_list = ' '.join(list(moviename_2))
        corpus = [moviename_1_list, moviename_2_list]  # 生成语料库
        vectoerizer = CountVectorizer(min_df=1, max_df=1.0, token_pattern='\\b\\w+\\b')  # 声明一个向量化工具
        tfidf_transformer = TfidfTransformer()  # 声明一个tf-idf转化器
        vectoerizer.fit(corpus)  # 根据语料集统计词袋
        X = vectoerizer.transform(corpus)  # 将语料集转化为词袋向量
        tfidf_transformer.fit(X.toarray())  # 根据语料集的词袋向量计算TF-IDF
        tfidf = tfidf_transformer.transform(X)  # 将语料集的词袋向量表示转换为TF-IDF向量表示
        tf_idf_list = tfidf.toarray()  # 将计算结果转化为列表
        moviename_1_tfidf = tf_idf_list[0]
        moviename_2_tfidf = tf_idf_list[1]
        similarity = cosine_similarity([moviename_1_tfidf, moviename_2_tfidf])[0][1]  # 计算tfidf余弦向量的夹角
        return similarity


class similarityCalculation(object):
    def __init__(self):
        self.compare_field = ["channel", "video_name", "actors", "directors", "publishyear"]
        self.confidence = Confidence()
        self.mapping = {False: 0, True: 1}
        self.same_set = [{"语"}, {"版"}, {"上"}, {"下"}, {""}]
        self.split_set = [{'粤', '语', '版'}, {'话', '版', '通', '普'}, {"新"}, {'外', '传'}]
        self.mongo_conn = MongoHelper()

    @retry(tries=3)
    def extract_similar_aid(self, dic):
        media_aid = dic["aid"]
        vendor = dic["vendor"]
        channel = dic["channel"]
        actor_list = dic["actors"]
        director_list = dic["directors"]
        # publishyear = dic["publishyear"]
        moviename = dic["video_name"]
        related_aid_dict = dict()
        if channel in ["电影", "电视剧"]:  # 同导演同演员同频道,预选集
            # aid_set = self.confidence.rule1(actor_set, director_set, channel_set)
            search_res = self.mongo_conn.db[settings.PUB_ALBUM].find({"vendor": {"$nin": [vendor, "13"]},
                                                                      "channel": channel, "actors": {"$in": actor_list},
                                                                      "directors": {"$in": director_list}})
            for media_dict in search_res:
                related_aid_dict[media_dict["aid"]] = media_dict

        elif channel in ["动漫", "少儿", "综艺", "纪录片"]:  # 找同名同频道
            # aid_set = self.confidence.rule2(channel_set, moviename_set)
            for media_dict in self.mongo_conn.db[settings.PUB_ALBUM].find({"vendor": {"$nin": [vendor, "13"]},
                                                                             "channel": channel,
                                                                             "video_name": moviename}):
                related_aid_dict[media_dict["aid"]] = media_dict

        similar_aid = set()
        for aid, dic2 in related_aid_dict.items():  # 找到所有与此条数据相似的其他牌照的aid
            if channel in ["动漫", "少儿", "综艺", "纪录片"]:
                actor_result = (set(dic["actors"]) & set(dic2["actors"])) != set()
                director_result = (set(dic["directors"]) & set(dic2["directors"])) != set()
                new_dic = {"AID1": dic["aid"],
                           "moviename1": moviename,
                           "AID2": dic2["aid"],
                           "moviename2": dic2["video_name"],
                           "Vendor比较": self.mapping[dic["vendor"] == dic2["vendor"]],
                           "Channel比较": self.mapping[dic["channel"] == dic2["channel"]],
                           "Moviename一级比较": self.mapping[dic["video_name"] == dic2["video_name"]],
                           "Moviename二级比较": self.confidence.compare_moviename_tfidf(dic, dic2),
                           "Actor比较": self.mapping[actor_result],
                           "Director比较": self.mapping[director_result],
                           "Year比较": self.mapping[dic["publishyear"] == dic2["publishyear"]],
                           "channel": channel}
                similar_aid.add(new_dic["AID2"])
            else:
                # movie等的进一步筛选
                moviename_tfidf = self.confidence.compare_moviename_tfidf(dic, dic2)
                moviename1_set = set(dic["video_name"])
                moviename2_set = set(dic2["video_name"])
                if len(moviename1_set) > len(moviename2_set):
                    res = moviename1_set - moviename2_set
                else:
                    res = moviename2_set - moviename1_set
                if res == set(): res = {""}
                # ----------等级二定义-----------
                # # if moviename_tfidf >= 0.5 and len(res) in [1, 0] and {list(res)[0]} in self.same_set:
                # flag = False
                # try:
                #     aa = int(list(res)[0])
                # except:
                #     pass
                # else:
                #     if aa > 1:
                #         flag = True
                # -----------------------------
                if moviename_tfidf == 1:  # 满足片名一级比较= 1，actor比较=1，director比较=1，该组合归为等级I
                    actor_result = (set(dic["actors"]) & set(dic2["actors"])) != set()
                    director_result = (set(dic["directors"]) & set(dic2["directors"])) != set()
                    new_dic = {"AID1": dic["aid"],
                               "moviename1": moviename,
                               "AID2": dic2["aid"],
                               "moviename2": dic2["video_name"],
                               "Vendor比较": self.mapping[dic["vendor"] == dic2["vendor"]],
                               "Channel比较": self.mapping[dic["channel"] == dic2["channel"]],
                               "Moviename一级比较": self.mapping[dic["video_name"] == dic2["video_name"]],
                               "Moviename二级比较": self.confidence.compare_moviename_tfidf(dic, dic2),
                               "Actor比较": self.mapping[actor_result],
                               "Director比较": self.mapping[director_result],
                               "Year比较": self.mapping[dic["publishyear"] == dic2["publishyear"]],
                               "channel": channel}
                    similar_aid.add(new_dic["AID2"])

                elif moviename_tfidf >= 0.5 and res in self.same_set:
                    actor_result = (set(dic["actors"]) & set(dic2["actors"])) != set()
                    director_result = (set(dic["directors"]) & set(dic2["directors"])) != set()
                    new_dic = {"AID1": dic["aid"],
                               "moviename1": moviename,
                               "AID2": dic2["aid"],
                               "moviename2": dic2["video_name"],
                               "Vendor比较": self.mapping[dic["vendor"] == dic2["vendor"]],
                               "Channel比较": self.mapping[dic["channel"] == dic2["channel"]],
                               "Moviename一级比较": self.mapping[dic["video_name"] == dic2["video_name"]],
                               "Moviename二级比较": self.confidence.compare_moviename_tfidf(dic, dic2),
                               "Actor比较": self.mapping[actor_result],
                               "Director比较": self.mapping[director_result],
                               "Year比较": self.mapping[dic["publishyear"] == dic2["publishyear"]],
                               "channel": channel}
                    similar_aid.add(new_dic["AID2"])

        if similar_aid:
            similar_aid = list(similar_aid)
            similar_aid.sort()

            existed_record = self.mongo_conn.find(coll=settings.PUB_ALBUM_SIMILAR_AID,
                                                  dict_filter={"aid": media_aid, "similar_aid": similar_aid})
            update_time = int(time.time() * 1000)
            if existed_record:
                self.mongo_conn.update(coll=settings.PUB_ALBUM_SIMILAR_AID, dict_filter={"aid": media_aid},
                                       dict_update={"$set": {"update_time": update_time}})
            else:
                self.mongo_conn.replace_one(coll=settings.PUB_ALBUM_SIMILAR_AID, dict_filter={"aid": media_aid},
                                            dict_replacement={"id": dic["id"], "video_name": dic["video_name"],
                                                              "aid": dic["aid"], "vendor": vendor, "channel": channel,
                                                              "actors": actor_list, "directors": director_list,
                                                              "similar_aid": list(similar_aid),
                                                              "update_time": update_time})
                self.update_album_status(aid=media_aid)

    @retry(tries=3)
    def extract_by_channel(self, vendor, channel):
        vendor_channel_media = self.mongo_conn.find(coll=settings.PUB_ALBUM,
                                                    dict_filter={"vendor": vendor, "channel": channel})
        bar = tqdm(total=vendor_channel_media.count(),
                   desc=f"已处理 vendor: {vendor} channel: {channel}", ncols=100)
        with ThreadPoolExecutor(max_workers=4) as executor:
            for dic in vendor_channel_media:
                f = executor.submit(self.extract_similar_aid, dic=dic)
                f.add_done_callback(lambda future: bar.update())
        executor.shutdown()
        bar.close()
        logging.info(f"统计完成  vendor:{vendor}, channel:{channel}")

    def start(self):
        start_time = int(time.time() * 1000)
        self.mongo_conn = MongoHelper()
        vendors = self.mongo_conn.db[settings.PUB_ALBUM].distinct("vendor")
        vendors = [v for v in vendors if v not in ("17", "13")]  # 13和15 不重复计算, todo 小米数据aid和其它牌照重复！！但是内容无相关性
        vendors.sort()

        for vendor in tqdm(vendors, desc="牌照方", ncols=100):
            vendor_channels = [c for c in self.mongo_conn.db[settings.PUB_ALBUM].distinct("channel", {"vendor": vendor})
                               if
                               c in ["电影", "电视剧", "动漫", "少儿", "综艺", "纪录片"]]
            vendor_channels.sort()
            for channel in vendor_channels:
                self.extract_by_channel(vendor=vendor, channel=channel)

        # 删除旧数据
        need_update_aid = list()
        for doc in self.mongo_conn.find(coll=settings.PUB_ALBUM_SIMILAR_AID,
                                        dict_filter={"update_time": {"$lt": start_time}}):
            need_update_aid.append(doc["aid"])
        self.mongo_conn.delete_many(coll=settings.PUB_ALBUM_SIMILAR_AID,
                                    dict_filter={"update_time": {"$lt": start_time}})

        for aid in need_update_aid:
            self.update_album_status(aid)
        logging.info(f"{'*' * 20} Success {'*' * 20}")

    def update_album_status(self, aid):
        # 重新上传
        status = self.mongo_conn.update(coll=settings.PUB_ALBUM, dict_filter={"aid": aid},
                                        dict_update={"$set": {"data_state": settings.PENDING}})
        return status


def update_similar_aid(hour="9", minute="40", run_now=False):
    sim_ca = similarityCalculation()
    scheduler = BlockingScheduler()
    next_run_time = undefined
    if run_now:
        next_run_time = datetime.datetime.now()

    scheduler.add_job(sim_ca.start, "cron", hour=hour, minute=minute, next_run_time=next_run_time)
    scheduler.start()


if __name__ == '__main__':
    update_similar_aid(run_now=True)
