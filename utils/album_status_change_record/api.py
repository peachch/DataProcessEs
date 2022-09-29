import time
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname((os.path.abspath(__file__))))))
import pandas
from flask import Flask, request, url_for, redirect
from pymongo import MongoClient
from collections import defaultdict
import settings
from datetime import datetime, timedelta

app = Flask(import_name=__name__, template_folder="./static")
pandas.set_option('colheader_justify', 'center')  # FOR TABLE <th>

mongo_conn = MongoClient(host=settings.MONGODB_HOST, port=settings.MONGODB_PORT)
history_coll = mongo_conn.tcl_search_media.album_change_record


@app.route(rule="/index", methods=["GET", ])
def index():
    """读取最近一次媒资上下架变化时间"""
    api_desc = f"""
    <h3>媒资上下架记录</h3>
    <p>1. 最近下架的媒资： <a href={url_for("status_history_group_by_status", status_code=0)}>{url_for("status_history_group_by_status", status_code=0)}\n</a></p>
    <p>2. 最近上架的媒资： <a href={url_for("status_history_group_by_status", status_code=1)}>{url_for("status_history_group_by_status", status_code=1)}\n</a></p>
    <p>3. 单条媒资上下架历史记录： <a href={url_for("status_history_media", media_id="17638874")}>{url_for("status_history_media", media_id="17638874")}\n</a></p>

    <p>* 默认时间范围是前一天的零点整到当前时间点，可以通过start_date参数指定时间范围，如 今天上架的媒资: <a href={url_for("status_history_group_by_status", status_code=1, start_date=datetime.today().date().strftime("%Y%m%d"))}>{url_for("status_history_group_by_status", status_code=1, start_date=datetime.today().date().strftime("%Y%m%d"))}</a></p>
    """
    return api_desc


@app.route(rule="/status_history/status/<status_code>", methods=["GET", ])
def status_history_group_by_status(status_code):
    """读取最近一次媒资上下架变化时间"""
    vendor = request.args.get("vendor")
    start_date = request.args.get("start_date", None)  # 开始日期
    if not start_date:
        # 没有指定日期默认查询昨天开始的结果
        start_date = datetime.today().date() - timedelta(days=1)
        start_date = start_date.strftime("%Y%m%d")

    start_timestamp_s = time.mktime(time.strptime(start_date, "%Y%m%d"))
    start_timestamp_ms = int(start_timestamp_s) * 1000
    filed_mapping = {"vendor": "牌照方", "aid": "媒资aid", "channel": "频道", "title": "标题", "status_old": "上架状态-旧",
                     "status_new": "上架状态-新", "upadte_time": "雷鸟更新时间",
                     "record_time": "记录时间"}
    status_records = history_coll.find(
        {"status_new": int(status_code), "timestamp": {"$gte": start_timestamp_ms}}).sort('timestamp', -1).limit(1000)
    change_history = []
    max_record_timestamp = defaultdict(lambda: 0)
    for status_record in status_records:
        # status_record = {"record_id": "32047294_1635414074042",
        #                  "timestamp": 1635414074042,
        #                  "album_id": "32047294", "status_old": 0,
        #                  "status_new": 1,
        #                  "media_old": {},
        #                  "media_new": {"vendor": "320", "aid": "47294", "title": "自拍", "enTitle": "",
        #                                "shortTitle": null, "fullPinyin": [""], "simplePinyin": [""], "aliases": [""],
        #                                "channel": "电影", "channelId": "001", "status": 1, "type": 1,
        #                                "brief": "佩雷斯夫妇在网上分享患有罕见病小儿子的生活短视频而名声大噪。古板的语文老师社交媒体上当“黑粉”，勾搭上网红脱口秀艺人。不善交际的青年弗洛里安通过提高约会软件的评分追到了身边的女神。中年成功人士罗曼被大数据和算法营销伺候得完全丧失了自我。社交网站个人信息泄露造成婚礼搁浅，宾客互殴……互联网带给人们的不只是美好，很多负面效应也会将人们推向崩溃甚至毁灭。 本片通过五个亦喜亦悲的故事，辛辣地讽刺了近年互联网的飞速发展对人们生活的深度渗透与侵蚀。本片获得2020年美国西南偏南电影节特别奖提名。",
        #                                "publishYear": 2020, "score": null, "payMode": 2,
        #                                "vPic": "https://pic5.huanxi.com/8a9eb00f788b71220178d47e9692308e.jpg?x-oss-process=image/resize,m_fill,h_664,w_496",
        #                                "vPicMd5": "d090759d7948252d4eda391c2b1a6c39",
        #                                "hPic": "https://pic4.huanxi.com/8a9eb00f788b71220178d47b7fdb2f50.jpg",
        #                                "hPicMd5": "73c3a51c996cd72aaaabc9cf05cae7d3",
        #                                "updateTime": "2021-04-19 16:30:59", "total": 1, "last": "1", "season": null,
        #                                "completed": true, "regions": ["法国"], "languages": ["法语"], "tags": ["喜剧"],
        #                                "actors": ["布朗什·加丁", "艾尔莎·泽贝斯坦", "芬妮·西德尼", "费尼肯·欧菲尔德", "阿尔玛·佐杜洛夫斯基", "马克·弗赖兹",
        #                                           "马克斯·布博里尔", "马克桑斯·蒂阿尔", "马努·佩埃特"],
        #                                "directors": ["托马斯·彼得葛恩", "特里斯坦·奥鲁埃", "范尼·勒巴斯克", "谢西尔·杰布拉", "马克·费杜斯"],
        #                                "producers": [""], "copyright": [""], "characters": [""], "awards": [""],
        #                                "params": "{\"behavior\":\"activity\",\"action\":\"com.tcl.vod.action.thirdappdetail\",\"package_name\":\"com.tcl.vod\",\"extra_map\":[{\"key\":\"videoId\",\"type\":\"string\",\"value\":\"47294\"},{\"key\":\"vendorId\",\"type\":\"int\",\"value\":\"320\"}]}",
        #                                "ext": {}, "sysTime": "2021-10-25 14:33:14", "publishDate": "",
        #                                "imp_date": "20211027", "data_source": "oss @v1.0",
        #                                "data_digest": "bef676552312326f3847162c9449fe74", "id": "32047294",
        #                                "data_state": 0, "data_timestamp": 1635414073959}}
        record = dict(vendor=status_record["media_new"]["vendor"],
                      aid=status_record["media_new"]["aid"],
                      channel=status_record["media_new"]["channel"],
                      title=f'<a href="  {url_for("status_history_media", media_id=status_record["album_id"])}" title="{str(status_record["media_new"])}">{status_record["media_new"]["title"]}</a>',
                      status_old=status_record["status_old"],
                      status_new=status_record["status_new"],
                      upadte_time=status_record["media_new"]["sysTime"],
                      record_time=time.strftime("%Y-%m-%d %H:%M:%S",
                                                time.localtime(int(status_record["timestamp"] / 1000))))

        data_id = status_record["album_id"]
        record_timestamp = status_record["timestamp"]
        if record_timestamp > max_record_timestamp[data_id]:
            change_history.append(record)
            max_record_timestamp[data_id] = record_timestamp

    df = pandas.DataFrame(data=change_history,
                          columns=list(filed_mapping.keys()))

    print(df)
    html_string = '''
    <html>
      <head><title>媒资最新上下架记录</title><meta charset="utf-8"></head>
      <link rel="stylesheet" type="text/css" href="/static/df_style.css"/>
      <body>
        {table}
      </body>
    </html>.
    '''
    df = df.rename(columns=filed_mapping)
    html = html_string.format(table=df.to_html(classes='mystyle', escape=False))
    return html


@app.route(rule="/status_history/media/<media_id>", methods=["GET", ])
def status_history_media(media_id):
    """查询单个媒资历史上下架信息"""
    vendor = request.args.get("vendor")
    start_date = request.args.get("start_date", None)  # 开始日期
    if not start_date:
        # 没有指定日期默认查询昨天开始的结果
        start_date = datetime.today().date() - timedelta(days=30)
        start_date = start_date.strftime("%Y%m%d")

    start_timestamp_s = time.mktime(time.strptime(start_date, "%Y%m%d"))
    start_timestamp_ms = int(start_timestamp_s) * 1000
    filed_mapping = {"media_id": "媒资ID", "channel": "频道", "title": "标题", "status_old": "上架状态-旧",
                     "status_new": "上架状态-新", "data_source": "数据源", "upadte_time": "雷鸟更新时间",
                     "record_time": "记录时间"}
    status_records = history_coll.find({"album_id": media_id, "timestamp": {"$gte": start_timestamp_ms}}).sort('timestamp', -1).limit(1000)
    change_history = []
    for status_record in status_records:
        # status_record = {"record_id": "32047294_1635414074042",
        #                  "timestamp": 1635414074042,
        #                  "album_id": "32047294", "status_old": 0,
        #                  "status_new": 1,
        #                  "media_old": {},
        #                  "media_new": {"vendor": "320", "aid": "47294", "title": "自拍", "enTitle": "",
        #                                "shortTitle": null, "fullPinyin": [""], "simplePinyin": [""], "aliases": [""],
        #                                "channel": "电影", "channelId": "001", "status": 1, "type": 1,
        #                                "brief": "佩雷斯夫妇在网上分享患有罕见病小儿子的生活短视频而名声大噪。古板的语文老师社交媒体上当“黑粉”，勾搭上网红脱口秀艺人。不善交际的青年弗洛里安通过提高约会软件的评分追到了身边的女神。中年成功人士罗曼被大数据和算法营销伺候得完全丧失了自我。社交网站个人信息泄露造成婚礼搁浅，宾客互殴……互联网带给人们的不只是美好，很多负面效应也会将人们推向崩溃甚至毁灭。 本片通过五个亦喜亦悲的故事，辛辣地讽刺了近年互联网的飞速发展对人们生活的深度渗透与侵蚀。本片获得2020年美国西南偏南电影节特别奖提名。",
        #                                "publishYear": 2020, "score": null, "payMode": 2,
        #                                "vPic": "https://pic5.huanxi.com/8a9eb00f788b71220178d47e9692308e.jpg?x-oss-process=image/resize,m_fill,h_664,w_496",
        #                                "vPicMd5": "d090759d7948252d4eda391c2b1a6c39",
        #                                "hPic": "https://pic4.huanxi.com/8a9eb00f788b71220178d47b7fdb2f50.jpg",
        #                                "hPicMd5": "73c3a51c996cd72aaaabc9cf05cae7d3",
        #                                "updateTime": "2021-04-19 16:30:59", "total": 1, "last": "1", "season": null,
        #                                "completed": true, "regions": ["法国"], "languages": ["法语"], "tags": ["喜剧"],
        #                                "actors": ["布朗什·加丁", "艾尔莎·泽贝斯坦", "芬妮·西德尼", "费尼肯·欧菲尔德", "阿尔玛·佐杜洛夫斯基", "马克·弗赖兹",
        #                                           "马克斯·布博里尔", "马克桑斯·蒂阿尔", "马努·佩埃特"],
        #                                "directors": ["托马斯·彼得葛恩", "特里斯坦·奥鲁埃", "范尼·勒巴斯克", "谢西尔·杰布拉", "马克·费杜斯"],
        #                                "producers": [""], "copyright": [""], "characters": [""], "awards": [""],
        #                                "params": "{\"behavior\":\"activity\",\"action\":\"com.tcl.vod.action.thirdappdetail\",\"package_name\":\"com.tcl.vod\",\"extra_map\":[{\"key\":\"videoId\",\"type\":\"string\",\"value\":\"47294\"},{\"key\":\"vendorId\",\"type\":\"int\",\"value\":\"320\"}]}",
        #                                "ext": {}, "sysTime": "2021-10-25 14:33:14", "publishDate": "",
        #                                "imp_date": "20211027", "data_source": "oss @v1.0",
        #                                "data_digest": "bef676552312326f3847162c9449fe74", "id": "32047294",
        #                                "data_state": 0, "data_timestamp": 1635414073959}}
        record = dict(media_id=status_record["album_id"],
                      channel=status_record["media_new"]["channel"],
                      title=f'<a href="{status_record["media_new"]["vPic"]}" title="{str(status_record["media_new"])}">{status_record["media_new"]["title"]}</a>',
                      status_old=status_record["status_old"],
                      status_new=status_record["status_new"],
                      data_source=status_record["media_new"]["data_source"],
                      upadte_time=status_record["media_new"]["sysTime"],
                      record_time=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(status_record["timestamp"] / 1000))))

        change_history.append(record)
    df = pandas.DataFrame(data=change_history, columns=list(filed_mapping.keys()))

    print(df)
    # 声明页面编码pandas读取时不会出现乱码
    html_string = '''
    <html>
      <head><title>单条媒资上下架历史记录</title><meta charset="utf-8"></head>
      <link rel="stylesheet" type="text/css" href="/static/df_style.css"/>
      <body>
        {table}
      </body>
    </html>.
    '''
    df = df.rename(columns=filed_mapping)
    html = html_string.format(table=df.to_html(classes='mystyle', escape=False))
    return html


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=0000, threaded=True, debug=False)
