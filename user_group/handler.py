
import datetime
import logging
import os
import sys
import time
from abc import abstractmethod
from collections import defaultdict
from concurrent.futures._base import wait
from concurrent.futures.thread import ThreadPoolExecutor
from functools import partial
from threading import Thread, Lock

import elasticsearch
from apscheduler.schedulers.blocking import BlockingScheduler
from elasticsearch import helpers
from tqdm import tqdm

from common.es_helper import ElasticHelper

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.oss_helper import OssHelper
import settings
from utils.event_core import Event, EventManager
from user_group.models import Operation2Elastic, User, Group

LOCK = Lock()
UPDATE_USER_GROUP = ""
UPDATE_USERS = ""
UPDATE_RECOMMEND_USERS = ""
ES_SERVERS = settings.USER_GROUP_ES_SERVERS  # 需要同步到的服务器


class Dataset(object):
    url = "http://ip:pory/oss/hive/list"
    key = ""
    key_secret = ""
    data_type = ""
    event_type = None

    def __init__(self):
        self.oss_helper = OssHelper(url=self.url, key=self.key, key_secret=self.key_secret, data_type=self.data_type)
        assert self.event_type is not None

    def update(self, date):
        try:
            files_path = self.oss_helper.start_download(date)
        except Exception as e:
            return None

        if files_path:
            event = Event(event_type=self.event_type)
            event.dict["date"] = date
            event.dict["oss_helper"] = self.oss_helper
            return event


class DnumDataset(Dataset):
    def __init__(self):
        self.data_type = ""  # 雷鸟分众数据
        self.event_type = UPDATE_USER_GROUP
        super(DnumDataset, self).__init__()


class UserDataset(Dataset):
    def __init__(self):
        self.data_type = ""  # 语音用户
        self.event_type = UPDATE_USERS
        super(UserDataset, self).__init__()


class GroupHandler(object):
    group_id = None
    desc = None

    def __init__(self, group_id):
        self.group_id = group_id
        group = Group(group_id=self.group_id, desc=self.desc, update_timestamp=int(time.time() * 1000)).save()
        self.group = group

    def add_user_to_group(self, user_dict):
        """添加用户分组信息"""
        user = User.objects(user_id=user_dict["user_id"]).first()
        if user and self.group not in user.groups:
            user_info = dict(push__groups=self.group,
                             version_code=user_dict.get("version_code"),
                             client_type=user_dict.get("client_type"),
                             package_name=user_dict.get("package_name"),
                             update_timestamp=int(time.time() * 1000))
            user_info_update = {k: v for k, v in user_info.items() if v}
            user.update(**user_info_update)

        else:
            user = User(
                user_id=user_dict["user_id"],
                version_code=user_dict.get("version_code", -1),
                client_type=user_dict.get("client_type", ""),
                package_name=user_dict.get("package_name", ""),
                groups=[self.group],
                update_timestamp=int(time.time() * 1000)
            )
            user.save()
        return user

    def remove_user_from_group(self, user_id):
        """移除用户分组"""
        user = User.objects(user_id=user_id).first()
        if self.group in user.groups:
            if len(user.groups) == 1:
                status = User.delete(user)
            else:
                status = user.update(pull__groups=self.group, update_timestamp=int(time.time() * 1000))
            return status

    @abstractmethod
    def get_new_group_users(self, event) -> dict:
        raise NotImplementedError

    @staticmethod
    def update_bar(bar):
        global LOCK
        with LOCK:
            bar.update()

    def compare_old_group_users(self, new_group_users: dict):
        """为优化内存，读取旧分众用户时完成数据对比"""
        users_remove = set()
        bar = tqdm(desc=f"reading old group :{self.group_id}")
        for user in User.objects(groups__in=[self.group_id, ]).no_cache():  # mongoengine默认缓存查询结果,导致内存占用过高
            user_id = user.user_id

            # 移除重复用户id
            if user_id in new_group_users:
                new_group_users.pop(user_id)
            else:
                users_remove.add(user.user_id)  # 只读取id,减少内存消耗
            bar.update()

        bar.close()
        users_add = new_group_users
        return users_remove, users_add

    def process_event(self, event):
        # 根据事件信息更新对应分组
        new_group_users: dict = self.get_new_group_users(event)

        # 只保留差集，降低内存占用
        users_remove, users_add = self.compare_old_group_users(new_group_users)
        executor = ThreadPoolExecutor(max_workers=24)
        doing = []
        bar_create = tqdm(desc=f"adding user to group {self.group_id}", total=len(users_add))
        for user_dict in users_add.values():
            f = executor.submit(self.add_user_to_group, user_dict)
            f.add_done_callback(lambda fu: self.update_bar(bar=bar_create))
            doing.append(f)
            if len(doing) >= 24000:
                wait(doing)
                doing = []
        bar_create.close()

        bar_remove = tqdm(desc=f"remove user from group {self.group_id}", total=len(users_remove))
        for user_id in users_remove:
            f = executor.submit(self.remove_user_from_group, user_id)
            f.add_done_callback(lambda fu: self.update_bar(bar=bar_remove))
            doing.append(f)
            if len(doing) >= 24000:
                wait(doing)
                doing = []
        bar_remove.close()
        executor.shutdown()
        logging.info(f"group {self.group_id}, user count {User.objects(groups__in=[self.group_id, ]).count()}")


class Group2115(GroupHandler):
    def __init__(self):
        self.group_id = ""
        self.desc = "【备注】"
        super().__init__(group_id=self.group_id)

    def get_new_group_users(self, event):
        # 卖场投诉
        blacklist_path = os.path.join(settings.BASE_DIR, "user_group", "blacklist.txt")
        with open(blacklist_path, "r", encoding="utf-8") as f:
            blacklist = set(f.read().splitlines())

        date = event.dict["date"]
        oss_helper = event.dict["oss_helper"]
        # 读取相关分组数据
        data_iterator = oss_helper.read_parquet_date(date=date, query="id == ''")     # 2115替换为100191
        new_group_users = dict()
        bar = tqdm(desc=f"reading new group :{self.group_id}")
        for user_dict in data_iterator:
            if not user_dict["dnum"]:
                continue

            # 增加黑名单过滤卖场机器
            if user_dict["dnum"] in blacklist:
                continue

            user_dict["user_id"] = user_dict["dnum"]
            user_dict["version_code"] = int(user_dict["latest_version_code"]) if user_dict["latest_version_code"] else 0
            if user_dict["dnum"] not in new_group_users:
                new_group_users[user_dict["dnum"]] = user_dict
                bar.update()
        bar.close()
        return new_group_users


class Group100597(GroupHandler):
    def __init__(self):
        self.group_id = ""
        self.desc = "【备注】"
        super().__init__(group_id=self.group_id)

    def get_new_group_users(self, event):
        date = event.dict["date"]
        oss_helper = event.dict["oss_helper"]
        # 读取相关分组数据
        data_iterator = oss_helper.read_parquet_date(date=date, query="id == '100597'")
        new_group_users = dict()
        bar = tqdm(desc=f"reading new group :{self.group_id}")
        for user_dict in data_iterator:
            if not user_dict["dnum"]:
                continue

            user_dict["user_id"] = user_dict["dnum"]
            user_dict["version_code"] = int(user_dict["latest_version_code"]) if user_dict["latest_version_code"] else 0
            if user_dict["dnum"] not in new_group_users:
                new_group_users[user_dict["dnum"]] = user_dict
                bar.update()
        bar.close()
        return new_group_users


class Group100108(GroupHandler):
    def __init__(self):
        self.group_id = ""
        self.desc = "【备注】"
        super().__init__(group_id=self.group_id)

    def get_new_group_users(self, event):
        date = event.dict["date"]
        oss_helper = event.dict["oss_helper"]
        # 读取相关分组数据
        data_iterator = oss_helper.read_parquet_date(date=date, query="id == '100108'")
        new_group_users = dict()
        bar = tqdm(desc=f"reading new group :{self.group_id}")
        for user_dict in data_iterator:
            if not user_dict["dnum"]:
                continue

            user_dict["user_id"] = user_dict["dnum"]
            user_dict["version_code"] = int(user_dict["latest_version_code"]) if user_dict["latest_version_code"] else 0
            if user_dict["dnum"] not in new_group_users:
                new_group_users[user_dict["dnum"]] = user_dict
                bar.update()
        bar.close()
        return new_group_users


class Group100205(GroupHandler):
    def __init__(self):
        self.group_id = ""
        self.desc = "【】"
        super().__init__(group_id=self.group_id)

    def get_new_group_users(self, event):
        date = event.dict["date"]
        oss_helper = event.dict["oss_helper"]
        # 读取相关分组数据
        data_iterator = oss_helper.read_parquet_date(date=date, query="id == '100205'")
        new_group_users = dict()
        bar = tqdm(desc=f"reading new group :{self.group_id}")
        for user_dict in data_iterator:
            if not user_dict["dnum"]:
                continue

            user_dict["user_id"] = user_dict["dnum"]
            user_dict["version_code"] = int(user_dict["latest_version_code"]) if user_dict["latest_version_code"] else 0
            if user_dict["dnum"] not in new_group_users:
                new_group_users[user_dict["dnum"]] = user_dict
                bar.update()
        bar.close()
        return new_group_users


class Group100964(GroupHandler):
    def __init__(self):
        self.group_id = ""
        self.desc = ""
        super().__init__(group_id=self.group_id)

    def get_new_group_users(self, event):
        date = event.dict["date"]
        oss_helper = event.dict["oss_helper"]
        # 读取相关分组数据
        data_iterator = oss_helper.read_parquet_date(date=date, query="id == '100964'")
        new_group_users = dict()
        bar = tqdm(desc=f"reading new group :{self.group_id}")
        for user_dict in data_iterator:
            if not user_dict["dnum"]:
                continue

            user_dict["user_id"] = user_dict["dnum"]
            user_dict["version_code"] = int(user_dict["latest_version_code"]) if user_dict["latest_version_code"] else 0
            if user_dict["dnum"] not in new_group_users:
                new_group_users[user_dict["dnum"]] = user_dict
                bar.update()
        bar.close()
        return new_group_users


class Group3152(GroupHandler):
    def __init__(self):
        self.group_id = ""
        self.desc = ""
        super().__init__(group_id=self.group_id)

    def get_new_group_users(self, event):
        date = event.dict["date"]
        oss_helper = event.dict["oss_helper"]
        # 读取相关分组数据
        data_iterator = oss_helper.read_parquet_date(date=date, query="id == '3152'")
        new_group_users = set()
        bar = tqdm(desc=f"reading new group :{self.group_id}")
        for user_dict in data_iterator:
            if not user_dict["dnum"]:
                continue

            user_dict["user_id"] = user_dict["dnum"]
            user_dict["version_code"] = int(user_dict["latest_version_code"]) if user_dict["latest_version_code"] else 0
            if user_dict["dnum"] not in new_group_users:
                new_group_users.add(user_dict["user_id"])
                bar.update()
        bar.close()

        new_group_users_dict = {k: {"user_id": k} for k in new_group_users}
        return new_group_users_dict


class Group3224(GroupHandler):
    def __init__(self):
        self.group_id = ""
        self.desc = ""
        super().__init__(group_id=self.group_id)

    def get_new_group_users(self, event):
        date = event.dict["date"]
        oss_helper = event.dict["oss_helper"]
        # 读取相关分组数据
        data_iterator = oss_helper.read_parquet_date(date=date, query="id == '3224'")
        new_group_users = set()
        bar = tqdm(desc=f"reading new group :{self.group_id}")
        for user_dict in data_iterator:
            if not user_dict["dnum"]:
                continue

            user_dict["user_id"] = user_dict["dnum"]
            user_dict["version_code"] = int(user_dict["latest_version_code"]) if user_dict["latest_version_code"] else 0
            if user_dict["dnum"] not in new_group_users:
                new_group_users.add(user_dict["user_id"])
                bar.update()
        bar.close()
        new_group_users_dict = {k: {"user_id": k} for k in new_group_users}

        return new_group_users_dict


class Group101432(GroupHandler):
    def __init__(self):
        self.group_id = ""
        self.desc = "【】"
        super().__init__(group_id=self.group_id)

    def get_new_group_users(self, event):
        date = event.dict["date"]
        oss_helper = event.dict["oss_helper"]
        # 读取相关分组数据
        data_iterator = oss_helper.read_parquet_date(date=date, query="id == '101432'")
        new_group_users = set()
        bar = tqdm(desc=f"reading new group :{self.group_id}")
        for user_dict in data_iterator:
            if not user_dict["dnum"]:
                continue

            user_dict["user_id"] = user_dict["dnum"]
            user_dict["version_code"] = int(user_dict["latest_version_code"]) if user_dict["latest_version_code"] else 0
            if user_dict["dnum"] not in new_group_users:
                new_group_users.add(user_dict["user_id"])
                bar.update()
        bar.close()
        new_group_users_dict = {k: {"user_id": k} for k in new_group_users}

        return new_group_users_dict


class Group101881(GroupHandler):
    def __init__(self):
        self.group_id = ""
        self.desc = "【】"
        super().__init__(group_id=self.group_id)

    def get_new_group_users(self, event):
        date = event.dict["date"]
        oss_helper = event.dict["oss_helper"]
        # 读取相关分组数据
        data_iterator = oss_helper.read_parquet_date(date=date, query="id == '1'")
        new_group_users = set()
        bar = tqdm(desc=f"reading new group :{self.group_id}")
        for user_dict in data_iterator:
            if not user_dict["dnum"]:
                continue

            user_dict["user_id"] = user_dict["dnum"]
            user_dict["version_code"] = int(user_dict["latest_version_code"]) if user_dict["latest_version_code"] else 0
            if user_dict["dnum"] not in new_group_users:
                new_group_users.add(user_dict["user_id"])
                bar.update()
        bar.close()
        new_group_users_dict = {k: {"user_id": k} for k in new_group_users}

        return new_group_users_dict


class Group210530(GroupHandler):
    def __init__(self):
        self.group_id = ""
        self.desc = ""
        super().__init__(group_id=self.group_id)

    def get_new_group_users(self, event):
        date = event.dict["date"]
        oss_helper = event.dict["oss_helper"]
        # 读取相关分组数据
        data_iterator = oss_helper.read_parquet_date(date=date)
        new_group_users = dict()
        bar = tqdm(desc=f"reading new group :{self.group_id}")
        for user_dict in data_iterator:
            if not user_dict["dnum"]:
                continue
            user_dict["user_id"] = user_dict["dnum"]
            user_dict["version_code"] = 0
            if user_dict["dnum"] not in new_group_users:
                new_group_users[user_dict["dnum"]] = user_dict
                bar.update()
        bar.close()
        return new_group_users


class Group210923(GroupHandler):
    """
    推荐系统AB测试组1: 推荐系统用户组1，用户的泛搜query使用推荐模型(recall+ rank )服务。
    """

    def __init__(self):
        self.group_id = ""
        self.desc = "推荐系统用户组1，用户的泛搜query使用推荐模型(recall+ rank )服务。"
        super().__init__(group_id=self.group_id)

    def get_new_group_users(self, event):
        # 直接读取Es用户特征索引
        bar = tqdm(desc=f"reading new group :{self.group_id}")
        new_group_users = dict()
        dnum_recall_and_rank = event.dict["dnum_recall_and_rank"]

        for user in dnum_recall_and_rank:
            new_group_users[user] = {"user_id": user}
            bar.update()
        bar.close()
        return new_group_users


class Group210926(GroupHandler):
    """
    推荐系统AB测试组2: 推荐系统用户组2，用户的泛搜query使用推荐recall模型服务。
    """

    def __init__(self):
        self.group_id = ""
        self.desc = "推荐系统用户组2，用户的泛搜query使用推荐recall模型服务。"
        super().__init__(group_id=self.group_id)

    def get_new_group_users(self, event):
        # 直接读取Es用户特征索引
        bar = tqdm(desc=f"reading new group :{self.group_id}")
        new_group_users = dict()
        dnum_recall_without_rank = event.dict["dnum_recall_without_rank"]

        for user in dnum_recall_without_rank:
            new_group_users[user] = {"user_id": user}
            bar.update()
        bar.close()
        return new_group_users


class T0211020(GroupHandler):
    """媒资排它分众"""

    def __init__(self):
        self.group_id = ""
        self.desc = "用户搜索的资源，如果腾讯有，则屏蔽其他所有CP方的对应资源，仅展示腾讯资源。	"
        super().__init__(group_id=self.group_id)

    def get_new_group_users(self, event):
        date = event.dict["date"]
        oss_helper = event.dict["oss_helper"]
        # 读取相关分组数据
        data_iterator = oss_helper.read_parquet_date(date=date)
        new_group_users = dict()
        bar = tqdm(desc=f"reading new group :{self.group_id}")
        for user_dict in data_iterator:
            if not user_dict["dnum"]:
                continue
            user_dict["user_id"] = user_dict["dnum"]
            user_dict["version_code"] = 0
            if user_dict["dnum"] not in new_group_users:
                new_group_users[user_dict["dnum"]] = user_dict
                bar.update()
        bar.close()
        return new_group_users


def event_factory(dataset: Dataset, event_manager: EventManager):
    """oss拉取新数据"""
    wait_seconds = 60 * 60 * 2  # 等待2h
    sync_completed_date = defaultdict(lambda: False)
    # 数据上传完的时间不确定,定时检测尝试。 大概..每天下午两点左右
    while True:
        time.sleep(0.4)
        current_date = datetime.datetime.now().strftime("%Y%m%d")  # 从当前日期开始同步
        # current_date = "20210810"

        if sync_completed_date[current_date]:
            continue
        else:
            while not sync_completed_date[current_date]:
                # 数据上传完的时间不确定,定时检测尝试。 实际大概..每天下午两点左右
                time.sleep(0.1)
                event = dataset.update(date=current_date)

                if event:
                    event_manager.SendEvent(event)
                    sync_completed_date[current_date] = True
                    break
                else:
                    # 等待重试
                    next_retry_time = datetime.datetime.now() + datetime.timedelta(seconds=wait_seconds)
                    next_retry_timestamp = next_retry_time.timestamp()
                    while time.time() <= next_retry_timestamp:
                        tqdm.write(f'Current time：{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}, '
                                   f'Next Retry time: {next_retry_time.strftime("%Y-%m-%d %H:%M:%S")}', end="\r")
                        time.sleep(30)


def update_recommand_user_group(event_manager: EventManager):
    """读取推荐索引"""
    print("reading recommend users")
    es_client = elasticsearch.Elasticsearch(hosts="http://ip:port/", timeout=1000)
    dnum_rank_cursor = helpers.scan(es_client, index="dnum_idx", size=10000,
                                    query={"query": {"match_all": {}}, "_source": ["dnum"]})
    dnum_rank = set()
    bar = tqdm(desc="reading dnum_index")
    for hit in dnum_rank_cursor:
        dnum_rank.add(str(hit["_source"]["dnum"]))
        bar.update()

    dnum_indexed = helpers.scan(es_client, index="dnum_index", size=10000,
                                query={"query": {"match_all": {}}, "_source": ["dnum"]})
    dnum_all = set()
    dnum_all_bar = tqdm(desc="dnum_index")
    for hit in dnum_indexed:
        dnum_all.add(str(hit["_source"]["dnum"]))
        dnum_all_bar.update()

    dnum_recall_cursor = helpers.scan(es_client, index="user_tag_profile", size=10000,
                                      query={"query": {"match_all": {}}, "_source": ["dnum"]})
    dnum_recall = set()
    dnum_recall_bar = tqdm(desc="user_tag_profile")
    for hit in dnum_recall_cursor:
        dnum_recall.add(str(hit["_source"]["dnum"]))
        dnum_recall_bar.update()

    dnum_recall_and_rank = dnum_recall & dnum_rank
    dnum_recall_without_rank = dnum_all - dnum_rank

    event = Event(event_type=UPDATE_RECOMMEND_USERS)
    event.dict["dnum_recall_and_rank"] = dnum_recall_and_rank
    event.dict["dnum_recall_without_rank"] = set(list(dnum_recall_without_rank)[:50000])
    event_manager.SendEvent(event)
    return


def scheduler_jobs(event_manager: EventManager):
    """每天定时更新一次推荐用户组"""
    scheduler = BlockingScheduler()
    scheduler.add_job(partial(update_recommand_user_group, event_manager=event_manager), "cron", hour=16, minute=59,
                      next_run_time=datetime.datetime.now())
    scheduler.start()


def start():
    """主函数"""
    elasticPusher = Operation2Elastic(es_hosts=ES_SERVERS)
    elasticPusher.start()
    event_manager = EventManager()
    event_manager.Start()
    event_manager.AddEventListener(type_=UPDATE_USERS, handler=T0211020().process_event)  # 腾讯排它

    event_manager.AddEventListener(type_=UPDATE_USER_GROUP, handler=Group3152().process_event)  # TVB牌照
    event_manager.AddEventListener(type_=UPDATE_USER_GROUP, handler=Group3224().process_event)  # 欢喜传媒
    # 暂时不上线  event_manager.AddEventListener(type_=UPDATE_USER_GROUP, handler=Group101432().process_event)  # 搜狐牌照
    event_manager.AddEventListener(type_=UPDATE_USER_GROUP, handler=Group101881().process_event)  # 新视听

    # *event_manager.AddEventListener(type_=UPDATE_RECOMMEND_USERS, handler=Group210923().process_event)
    # *event_manager.AddEventListener(type_=UPDATE_RECOMMEND_USERS, handler=Group210926().process_event)

    event_manager.AddEventListener(type_=UPDATE_USER_GROUP, handler=Group100108().process_event)
    # 暂时不上线 event_manager.AddEventListener(type_=UPDATE_USER_GROUP, handler=Group100597().process_event)
    event_manager.AddEventListener(type_=UPDATE_USER_GROUP, handler=Group100205().process_event)
    event_manager.AddEventListener(type_=UPDATE_USER_GROUP, handler=Group2115().process_event)

    event_manager.AddEventListener(type_=UPDATE_USER_GROUP, handler=Group100964().process_event)  # 大健康
    event_manager.AddEventListener(type_=UPDATE_USERS, handler=Group210530().process_event)  # 运营位

    group_dataset = DnumDataset()  # 同步分众数据集
    users_dataset = UserDataset()  # 数据组提供用户信息

    # *#推荐系统分众定时更新
    # *#thread_recommend = Thread(target=scheduler_jobs, args=(event_manager,), name="update_recommend_users")
    # *#thread_recommend.start()

    # oss下载分众数据
    for dataset in [users_dataset, group_dataset]:
        thread = Thread(target=event_factory, args=(dataset, event_manager), name=dataset.data_type)
        thread.start()


if __name__ == '__main__':
    start()
