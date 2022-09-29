import logging
import os
import sys

import requests
from tqdm import tqdm

from controler_media_sync.log_sync import SyncLogHandler
from pipeline.update_field.update_heat import update_heat
from pipeline.update_field.update_repetitive_vendor import update_repetitive_vendor
from pipeline.update_field.update_similar_aid import update_similar_aid
from user_group.models import ES_SERVERS
from utils.mongo2elastic import Mango2Elastic
from utils.set_field_handler import FieldHacker
from utils.reprocess_media import reprocess_album as reprocess_media_album
from utils.reprocess_media import reprocess_video as reprocess_media_video
from pipeline.nodes_vendor import *
from pipeline.core.engine import worker
from utils.utils_class import get_pid
import click
from controler_media_sync.media_sync import SyncAlbumHandler, SyncVideoHandler
import setproctitle
from controler_media_sync.search_log_kafka2es import SyncKafkaUserLog


@click.group(invoke_without_command=True)
@click.option('--loglevel', help="设置日志级别,CRITICAL=50,ERROR=40,WARNING=30,INFO=20(默认),DEBUG=10",
              default="20", type=click.Choice(["10", "20", "30", "40", "50"]))
def cli(loglevel):
    # test_net()
    LOG_FORMAT = "%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s"
    logging.basicConfig(format=LOG_FORMAT, level=int(loglevel))
    logging.getLogger("elasticsearch").setLevel(logging.ERROR)
    logging.getLogger("pandas").setLevel(logging.ERROR)
    hostname = os.environ.get("HOSTNAME")
    if hostname not in ["", "", "Py-Data"]:
        logging.warning(f"hostname {hostname}, using settings file {settings.local_settings}")
    else:
        logging.info(f"hostname {hostname}, using settings file {settings.local_settings}")


@cli.command()
@click.option('--cursor_offset', help="指定起始消费指针位置", default="latest")
def import_album_kafka(cursor_offset):
    """从kafka增量更新专辑数据到Mongodb."""

    pid_name = "import_album_from_kafka"
    existed_pid_num = get_pid(name=pid_name)
    if existed_pid_num:
        logging.error(f"进程已存在,请勿重复启动! 进程ID: {existed_pid_num}, 进程名: {pid_name}")
        return

    setproctitle.setproctitle(pid_name)

    # 从配置文件读取 group_id, topic
    handler = SyncAlbumHandler(cursor_offset=cursor_offset)
    handler.sync_from_kafka()


@cli.command()
@click.option('--date', help="需要导入全量数据的日期", required=True)
@click.option('--query', help="pandas筛选条件", default=None)
def import_album_oss(date, query):
    """从oss一次性导入/更新所有专辑数据到Mongodb."""

    pid_name = "import_album_from_oss"
    existed_pid_num = get_pid(name=pid_name)
    if existed_pid_num:
        logging.error(f"进程已存在,请勿重复启动! 进程ID: {existed_pid_num}, 进程名: {pid_name}")
        return

    setproctitle.setproctitle(pid_name)

    # 从配置文件读取 group_id, topic
    handler = SyncAlbumHandler()
    if query is None:
        query = f"vendor == {list(settings.AVAILABLE_DATA[settings.ALBUM].keys())}"
    handler.sync_from_oss(date=date, query=query)


@cli.command()
@click.option('--cursor_offset', help="指定起始消费指针位置", default="latest")
def import_video_kafka(cursor_offset):
    """从kafka增量更新分集数据到Mongodb."""

    pid_name = "import_video_from_kafka"
    existed_pid_num = get_pid(name=pid_name)
    if existed_pid_num:
        logging.error(f"进程已存在,请勿重复启动! 进程ID: {existed_pid_num}, 进程名: {pid_name}")
        return

    setproctitle.setproctitle(pid_name)

    handler = SyncVideoHandler(cursor_offset=cursor_offset)
    handler.sync_from_kafka()


@cli.command()
@click.option('--date', help="需要导入全量数据的日期", required=True)
@click.option('--query', help="pandas筛选条件", default=None)
def import_video_oss(date, query):
    """从oss一次性导入/更新所有分集数据到Mongodb."""

    pid_name = "import_video_from_oss"
    existed_pid_num = get_pid(name=pid_name)
    if existed_pid_num:
        logging.error(f"进程已存在,请勿重复启动! 进程ID: {existed_pid_num}, 进程名: {pid_name}")
        return

    setproctitle.setproctitle(pid_name)

    # 从配置文件读取 group_id, topic
    handler = SyncVideoHandler()
    if query is None:
        query = f"vendor == {list(settings.AVAILABLE_DATA[settings.VIDEO].keys())}"
    handler.sync_from_oss(date=date, query=query)


@cli.command()
@click.option('--vendor', help=f'需要清洗的牌照方', required=True)
def start_album_pipeline(vendor):
    """读取Mongodb指定牌照专辑数据，处理到ES"""

    # 默认发布到生产和预生产
    pipeline = worker(vendor=vendor, media_type="album")


@cli.command()
@click.option('--vendor', help=f'需要清洗的牌照方', required=True)
def start_video_pipeline(vendor):
    """读取Mongodb指定牌照分集数据，处理到ES"""

    # 默认发布到生产和预生产
    pipeline = worker(vendor=vendor, media_type="video")


@cli.command()
@click.option('--hour', help='指定每天执行时间点.小时', default="9")
@click.option('--minute', help='指定每天执行时间点.分钟', default="40")
@click.option('--run_now', help='立即开始执行一次', default=False)
def similar_aid_scheduler(hour, minute, run_now):
    """批处理字段;计算similar_aid,每天指定时间点执行"""
    update_similar_aid(hour=hour, minute=minute, run_now=run_now)


@cli.command()
@click.option('--hour', help='指定每天执行时间点.小时', default="1")
@click.option('--minute', help='指定每天执行时间点.分钟', default="30")
@click.option('--run_now', help='立即开始执行一次', default=False)
def heat_scheduler(hour, minute, run_now):
    """批处理字段;导入更新heat字段,每天指定时间点执行"""
    update_heat(hour=hour, minute=minute, run_now=run_now)


@cli.command()
@click.option('--hour', help='指定每天执行时间点.小时', default="1")
@click.option('--minute', help='指定每天执行时间点.分钟', default="30")
@click.option('--run_now', help='立即开始执行一次', default=False)
def repetitive_vendor_scheduler(hour, minute, run_now):
    """批处理字段;导入更新heat字段,每天指定时间点执行"""
    update_repetitive_vendor(hour=hour, minute=minute, run_now=run_now)


@cli.command()
@click.option('--cursor_offset', help="指定起始消费指针位置", default="latest")
@click.option('--es_service_hosts', help="需要发布的环境 测试(test_env)/预生产(preproduction_env)/生产(production_env)",
              default=settings.ELK_ES)
def import_kafka_user_log(cursor_offset, es_service_hosts):
    """从kafka导入搜索日志到ES"""

    pid_name = "import_kafka_user_log_process"
    existed_pid_num = get_pid(name=pid_name)
    if existed_pid_num:
        print(f"进程已存在,请勿重复启动! 进程ID: {existed_pid_num}, 进程名: {pid_name}")
        return

    setproctitle.setproctitle(pid_name)

    handler = SyncKafkaUserLog(cursor_offset=cursor_offset, es_service_host=es_service_hosts)
    handler.start()


@cli.command()
@click.option('--start_date', help='传入需要同步的开始日期，格式例如20210220', type=str, required=True)
@click.option('--end_date', help='传入需要同步的结束日期，格式例如20210220', type=str, required=True)
def import_oss_user_log(start_date, end_date):
    """从oss导入搜索日志到ES"""
    handler = SyncLogHandler()
    handler.import_log_from_oss(start_date, end_date)


@cli.command()
@click.option('--collection', help="指定从mongodb中上传的媒资集合",
              type=click.Choice(['search_album_v3', 'recall_episodes_v3', 'search_episodes_v3']),
              multiple=False, required=True)
@click.option('--index_name', help="指定需要上传到的elasticsearch索引名", required=True)
@click.option('--elastic_url', help="指定elasticsearch地址", required=True)
@click.option('--vendor', help="指定需要同步的牌照方", default=None, required=True)
@click.option('--start_timestamp', help="指定开始更新的时间点", default=None, type=int)
def mongo_to_es(collection, index_name, elastic_url, vendor, start_timestamp):
    """从Mongodb中同步媒资到线上ES索引"""
    # python cli.py mongo-to-es --collection tcl_search_album_test --index_name tcl_search_album_test --elastic_host 47.106.214.27 --elastic_port 8710 --vendor 310
    handler = Mango2Elastic(collection=collection, index_name=index_name, elastic_url=elastic_url)
    handler.start(vendor, start_timestamp)


@cli.command()
@click.option('--vendor', help="重新处理的指定牌照", required=True)
@click.option('--channel', help="重新处理指定的频道", default=None)
@click.option('--media_id', help="重试指定id", default=None)
@click.option('--sys_time', help="重试指定时间点以后的数据, 如 '2021-04-23 23:23:07'", default=None)
def reprocess_album(vendor, channel, media_id, sys_time):
    """重新处理专辑，修复bug或者其它需要重跑数据时使用"""
    reprocess_media_album(vendor=vendor, channel=channel, media_id=media_id, sys_time=sys_time)


@cli.command()
@click.option('--vendor', help="重新处理的指定牌照", required=True)
@click.option('--channel', help="重新处理指定的频道", default=None)
@click.option('--media_id', help="重试指定id", default=None)
@click.option('--sys_time', help="重试指定时间点以后的数据, 如 '2021-04-23 23:23:07'", default=None)
def reprocess_video(vendor, channel, media_id, sys_time):
    """重新处理分集，修复bug或者其它需要重跑数据时使用"""
    reprocess_media_video(vendor=vendor, channel=channel, media_id=media_id, sys_time=sys_time)


@cli.command()
def sync_user_group():
    """更新分众数据"""
    from user_group.handler import start
    start()


@cli.command()
@click.option('--group_id', help="重新处理的指定牌照", required=True)
def refresh_user_group(group_id):
    """刷新线上ES分组数据"""
    from user_group.models import Group

    group = Group.objects(group_id=group_id).first()
    group.refresh()


@cli.command()
@click.option('--user_id', help="需要设置分众的设备号", required=True)
@click.option('--group_id', help="需要设置成的分组,可以指定多个分组，用空格分隔,", nargs=0, required=True)
@click.argument('group_id', nargs=-1)
def set_user_group(user_id, group_id):
    """自定义当前环境用户分组"""

    if not user_id and group_id:
        return
    from user_group.models import Group
    from user_group.models import User
    from user_group.models import Operation2Elastic
    elasticPusher = Operation2Elastic(es_hosts=ES_SERVERS, wait_seconds=0.5)
    elasticPusher.setDaemon(True)
    elasticPusher.start()

    user = User.objects(user_id=user_id).first()
    groups = []
    for group in set(group_id):
        group_obj = Group.objects(group_id=group).first()
        if not group_obj:
            logging.error(f"not available group_id {group}")
            return
        groups.append(group_obj)

    if user:
        user.update(groups=groups, update_timestamp=int(time.time() * 1000))  # 更新到指定分组
    else:
        user = User(
            user_id=user_id,
            version_code=-1,
            client_type="",
            package_name="",
            groups=groups,
            update_timestamp=int(time.time() * 1000)
        )
        user.save()
        logging.info(f"created new user, user id {user_id}")
    logging.info(f"{user.to_dict()}")
    elasticPusher.join(timeout=4)
    sys.exit()


@cli.command()
@click.option('--media_set', help="需要更新的媒资id（vendor+aid）可以指定多个，用逗号分隔,", required=True)
@click.option('--field_name', help="指定需要更新的字段名",
              type=click.Choice(["fullpinyin", "simplepinyin", "aliases", "regions", "languages", "tags", "actors",
                                 "directors", "producers", "characters", "awards", "writer"]))
@click.option('--items_add', help="需要新增的媒资标签,可以指定多个分组，用逗号分隔")
@click.option('--items_remove', help="需要移除的媒资标签,可以指定多个分组，用逗号分隔")
def update_multi_value(media_set, field_name, items_add="", items_remove=""):
    """更新标注数据，新增或者移除媒资标签 python cli.py update-multi-value --media_set 15sbif609lnu6zd94,13sbif609lnu6zd94 --field_name tags --items_add 医疗,健康 --items_remove 医疗健康"""
    hacker = FieldHacker(field_name=field_name)
    media_set = media_set.replace("，", ",").replace("\'", "").replace("\"", "")
    assert "，" not in media_set
    media_set = [m.strip() for m in media_set.split(",")]
    if items_add:
        items_add = items_add.replace("，", ",").replace("\'", "").replace("\"", "")
        items_add = [t.strip() for t in items_add.split(",")]
    if items_remove:
        items_remove = items_remove.replace("，", ",").replace("\'", "").replace("\"", "")
        items_remove = [t.strip() for t in items_remove.split(",")]

    for media_id in tqdm(media_set):
        if items_add:
            hacker.add_items(media_id=media_id, items_add=items_add)
        if items_remove:
            hacker.remove_items(media_id=media_id, items_remove=items_remove)
        reprocess_media_album(vendor={"$in": list(settings.AVAILABLE_DATA["album"].keys())}, media_id=media_id)


@cli.command()
@click.option('--media_set', help="需要更新的媒资id（vendor+aid）可以指定多个，用逗号分隔,", required=True)
@click.option('--field_name', help="指定需要更新的字段名", type=click.Choice(['title', 'video_name', 'entitle', 'brief', 'publishdate', 'reference_time', 'publishyear', 'score', 'season']))
@click.option('--item_value', help="为字段指定新的值", required=True)
def update_single_value(media_set, field_name, item_value):
    """更新标注数据，指定特定值 python cli.py update-single-value --media_set 15sbif609lnu6zd94,13sbif609lnu6zd94 --field_name publishdate --item_value 20170730"""
    hacker = FieldHacker(field_name=field_name)
    media_set = media_set.replace("，", ",").replace("\'", "").replace("\"", "")
    assert "，" not in media_set
    media_set = [m.strip() for m in media_set.split(",")]

    for media_id in tqdm(media_set):
        hacker.fix_item(media_id=media_id, new_value=item_value)
        reprocess_media_album(vendor={"$in": list(settings.AVAILABLE_DATA["album"].keys())}, media_id=media_id)


if __name__ == '__main__':
    cli()
